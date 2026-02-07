package analyzer.service;

import analyzer.persistence.model.UserEventInteraction;
import analyzer.persistence.model.EventSimilarity;
import analyzer.persistence.repository.EventWeightSumProjection;
import analyzer.persistence.repository.UserEventInteractionRepository;
import analyzer.persistence.repository.EventSimilarityRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.PageRequest;
import org.springframework.stereotype.Service;
import ru.practicum.ewm.stats.avro.ActionTypeAvro;
import ru.practicum.ewm.stats.avro.EventSimilarityAvro;
import ru.practicum.ewm.stats.avro.UserActionAvro;

import java.time.ZoneOffset;
import java.util.*;
import java.util.stream.Collectors;

@Slf4j
@Service
@RequiredArgsConstructor
public class AnalyzerServiceJpaImpl implements AnalyzerService {
    private final UserEventInteractionRepository userEventInteractionRepository;
    private final EventSimilarityRepository eventSimilarityRepository;

    @Override
    public Map<Long, Double> getInteractionsCount(List<Long> eventIds) {
        if (eventIds == null || eventIds.isEmpty()) {
            return Map.of();
        }

        List<EventWeightSumProjection> rows = userEventInteractionRepository.sumWeightsByEventIds(eventIds);
        if (rows == null || rows.isEmpty()) {
            log.info("No interactions found for {} eventIds", eventIds.size());
            return Map.of();
        }

        Map<Long, Double> result = rows.stream()
                .filter(Objects::nonNull)
                .filter(r -> r.getEventId() != null && r.getSum() != null)
                .collect(Collectors.toMap(EventWeightSumProjection::getEventId, EventWeightSumProjection::getSum));

        log.info("Interactions sum calculated for {} events (requestedIds={})", result.size(), eventIds.size());
        return result;
    }

    @Override
    public Map<Long, Double> getSimilarEvents(Long eventId, Long userId, Integer limit) {
        if (eventId == null || userId == null || limit == null || limit <= 0) {
            return Map.of();
        }

        List<Long> interactedEventIds = userEventInteractionRepository.findRecentEventIdsByUserId(userId, limit);

        List<EventSimilarity> similarities = eventSimilarityRepository.findSimilarNotInteracted(
                interactedEventIds,
                eventId,
                PageRequest.of(0, limit)
        );

        Map<Long, Double> result = similarities.stream()
                .collect(Collectors.toMap(
                        s -> s.getEventAId().equals(eventId) ? s.getEventBId() : s.getEventAId(),
                        EventSimilarity::getScore,
                        (a, b) -> a,
                        LinkedHashMap::new
                ));

        log.info("Similar events for eventId={} userId={} -> {} results", eventId, userId, result.size());
        return result;
    }

    @Override
    public Map<Long, Double> getRecommendations(Long userId, Integer limit) {
        if (userId == null || limit == null || limit <= 0) {
            return Map.of();
        }

        List<Long> interactedEventIds = userEventInteractionRepository.findRecentEventIdsByUserId(userId, limit);

        if (interactedEventIds.isEmpty()) {
            log.info("No interactions found for userId={}, returning empty recommendations", userId);
            return Map.of();
        }

        List<EventSimilarity> candidateSimilarities = eventSimilarityRepository.findCandidatesNotInteracted(
                interactedEventIds,
                PageRequest.of(0, limit)
        );

        Map<Long, Double> recommendedEventsByScore = new LinkedHashMap<>();

        log.info("Building recommendations: userId={}, limit={}, recentInteractions={}, candidatesFetched={}",
                userId, limit, interactedEventIds.size(), candidateSimilarities.size());

        for (EventSimilarity similarity : candidateSimilarities) {
            Long candidateEventId = interactedEventIds.contains(similarity.getEventAId())
                    ? similarity.getEventBId()
                    : similarity.getEventAId();

            if (interactedEventIds.contains(candidateEventId) || recommendedEventsByScore.containsKey(candidateEventId)) {
                continue;
            }

            double predictedRating = predictEventRating(candidateEventId, userId, interactedEventIds);

            if (predictedRating < 0.0) {
                continue;
            }

            recommendedEventsByScore.put(candidateEventId, predictedRating);

            if (recommendedEventsByScore.size() >= limit) {
                break;
            }
        }

        log.info("Recommendations built: userId={}, requestedLimit={}, actualReturned={}",
                userId, limit, recommendedEventsByScore.size());

        return recommendedEventsByScore;
    }

    private double predictEventRating(Long candidateEventId, Long userId, List<Long> interactedEventIds) {
        List<EventSimilarity> similaritiesWithInteracted = eventSimilarityRepository.findSimilarToInteractedEvents(
                candidateEventId,
                interactedEventIds,
                PageRequest.of(0, 10)
        );

        if (similaritiesWithInteracted.isEmpty()) {
            log.info("No interacted events for candidateEventId={} userId={}, skip", candidateEventId, userId);
            return -1.0;
        }

        Map<Long, Double> similarityByInteractedEventId = similaritiesWithInteracted.stream()
                .collect(Collectors.toMap(
                        s -> s.getEventAId().equals(candidateEventId) ? s.getEventBId() : s.getEventAId(),
                        EventSimilarity::getScore,
                        (a, b) -> a
                ));

        List<UserEventInteraction> userInteractions =
                userEventInteractionRepository.findByUserIdAndEventIdIn(userId, similarityByInteractedEventId.keySet().stream().toList());

        Map<Long, Double> weightByEventId = userInteractions.stream()
                .filter(Objects::nonNull)
                .collect(Collectors.toMap(
                        UserEventInteraction::getEventId,
                        UserEventInteraction::getWeight,
                        (a, b) -> a
                ));

        double weightedScoreSum = 0.0;
        double similaritySum = 0.0;

        for (var entry : similarityByInteractedEventId.entrySet()) {
            Long interactedEventId = entry.getKey();
            double similarityScore = entry.getValue();

            Double interactionWeight = weightByEventId.get(interactedEventId);
            if (interactionWeight == null) continue;

            weightedScoreSum += similarityScore * interactionWeight;
            similaritySum += similarityScore;
        }

        if (similaritySum == 0.0) {
            log.info("Similarity sum is 0 for candidateEventId={} userId={}, skip", candidateEventId, userId);
            return -1.0;
        }

        double predicted = weightedScoreSum / similaritySum;

        log.info("Predicted rating: userId={}, candidateEventId={}, predicted={}, simSum={}, similarities={}",
                userId, candidateEventId, predicted, similaritySum, similarityByInteractedEventId.size());

        return predicted;
    }

    @Override
    public void processEventSimilarity(EventSimilarityAvro avro) {
        EventSimilarity similarity = eventSimilarityRepository.findByEventAIdAndEventBId(avro.getEventA(), avro.getEventB());

        if (similarity != null) {
            similarity.setScore(avro.getScore());
            similarity.setCalculatedAt(avro.getTimestamp().atOffset(ZoneOffset.UTC));
            eventSimilarityRepository.save(similarity);
            log.info("Similarity updated: {}", similarity);
        } else {
            similarity = eventSimilarityRepository.save(EventSimilarity.builder()
                    .eventAId(avro.getEventA())
                    .eventBId(avro.getEventB())
                    .score(avro.getScore())
                    .calculatedAt(avro.getTimestamp().atOffset(ZoneOffset.UTC))
                    .build()
            );
            log.info("Similarity created: {}", similarity);
        }
    }

    @Override
    public void processUserAction(UserActionAvro userActionAvro) {
        UserEventInteraction interaction = userEventInteractionRepository.findByUserIdAndEventId(
                userActionAvro.getUserId(),
                userActionAvro.getEventId()
        );

        double newWeight = getWeight(userActionAvro.getActionType());

        if (interaction != null) {
            if (interaction.getWeight() < newWeight) {
                interaction.setWeight(newWeight);
                interaction.setOccurredAt(userActionAvro.getTimestamp().atOffset(ZoneOffset.UTC));
                userEventInteractionRepository.save(interaction);
                log.info("Interaction updated: {}", interaction);
            }
        } else {
            interaction = UserEventInteraction.builder()
                    .userId(userActionAvro.getUserId())
                    .eventId(userActionAvro.getEventId())
                    .weight(newWeight)
                    .occurredAt(userActionAvro.getTimestamp().atOffset(ZoneOffset.UTC))
                    .build();

            userEventInteractionRepository.save(interaction);
            log.info("Interaction created: {}", interaction);
        }
    }

    private double getWeight(ActionTypeAvro actionType) {
        return switch (actionType) {
            case VIEW -> 0.4;
            case REGISTER -> 0.8;
            case LIKE -> 1.0;
        };
    }
}
