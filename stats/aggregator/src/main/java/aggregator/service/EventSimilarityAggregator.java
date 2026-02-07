package aggregator.service;

import aggregator.kafka.AggregatorKafkaClient;
import config.KafkaClientProperties;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.WakeupException;
import org.springframework.stereotype.Component;
import ru.practicum.ewm.stats.avro.ActionTypeAvro;
import ru.practicum.ewm.stats.avro.EventSimilarityAvro;
import ru.practicum.ewm.stats.avro.UserActionAvro;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
@Component
@RequiredArgsConstructor
public class EventSimilarityAggregator {
    private final KafkaClientProperties kafkaClientProperties;
    private final AggregatorKafkaClient kafkaClient;

    private final Map<Long, Map<Long, Double>> userWeightsByEventId = new HashMap<>();
    private final Map<Long, Double> totalWeightByEventId = new HashMap<>();
    private final SharedMinWeightSumCache sharedMinWeightsSumCache = new SharedMinWeightSumCache();

    public void start() {
        Runtime.getRuntime().addShutdownHook(new Thread(kafkaClient.getConsumer()::wakeup));
        log.info("Aggregator started. Consuming topic='{}', producing topic='{}'",
                kafkaClientProperties.getTopics().getUserActionsTopic(),
                kafkaClientProperties.getTopics().getEventSimilarityTopic());

        try {
            while (true) {
                ConsumerRecords<Void, UserActionAvro> polledActions =
                        kafkaClient.getConsumer().poll(kafkaClientProperties.getConsumer().getPollTimeoutMs());

                List<EventSimilarityAvro> similaritiesToPublish = process(polledActions);

                if (!similaritiesToPublish.isEmpty()) {
                    similaritiesToPublish.forEach(similarity -> {
                        log.info("Sending event similarity: {}", similarity);
                        kafkaClient.getProducer().send(
                                new ProducerRecord<>(kafkaClientProperties.getTopics().getEventSimilarityTopic(), null, similarity)
                        );
                    });
                }
            }
        } catch (WakeupException ignore) {

        } catch (Exception e) {
            log.error("Error during process topic:{}", kafkaClientProperties.getTopics().getEventSimilarityTopic(), e);
        } finally {
            try {
                kafkaClient.getProducer().flush();
                kafkaClient.getConsumer().commitSync();
            } finally {
                log.info("Closing aggregator consumer and producer...");
                kafkaClient.stop();
            }
        }
    }

    private List<EventSimilarityAvro> process(ConsumerRecords<Void, UserActionAvro> userActions) {
        List<EventSimilarityAvro> calculatedSimilarities = new ArrayList<>();

        for (ConsumerRecord<Void, UserActionAvro> actionRecord : userActions) {
            UserActionAvro action = actionRecord.value();

            Long userId = action.getUserId();
            Long primaryEventId = action.getEventId();
            double actionWeightForPrimaryEvent = getWeight(action.getActionType());

            if (userWeightsByEventId.containsKey(primaryEventId)) {
                Map<Long, Double> weightsByUserForPrimaryEvent = userWeightsByEventId.get(primaryEventId);

                double previousUserWeightOnPrimary = weightsByUserForPrimaryEvent.getOrDefault(userId, 0.0);
                double updatedUserWeightOnPrimary = Math.max(previousUserWeightOnPrimary, actionWeightForPrimaryEvent);

                if (updatedUserWeightOnPrimary != previousUserWeightOnPrimary) {
                    weightsByUserForPrimaryEvent.put(userId, updatedUserWeightOnPrimary);

                    double delta = updatedUserWeightOnPrimary - previousUserWeightOnPrimary;
                    totalWeightByEventId.put(
                            primaryEventId,
                            totalWeightByEventId.getOrDefault(primaryEventId, 0.0) + delta
                    );

                    for (Long otherEventId : userWeightsByEventId.keySet()) {
                        if (!primaryEventId.equals(otherEventId)) {
                            Double userWeightForOtherEvent = userWeightsByEventId.get(otherEventId).get(userId);

                            if (userWeightForOtherEvent != null) {
                                double previousMin = Math.min(previousUserWeightOnPrimary, userWeightForOtherEvent);
                                double updatedMin = Math.min(updatedUserWeightOnPrimary, userWeightForOtherEvent);
                                double minDelta = updatedMin - previousMin;

                                double sharedMinWeightSum =
                                        sharedMinWeightsSumCache.get(primaryEventId, otherEventId) + minDelta;

                                sharedMinWeightsSumCache.put(primaryEventId, otherEventId, sharedMinWeightSum);

                                double primaryNorm = Math.sqrt(totalWeightByEventId.get(primaryEventId));
                                double otherNorm = Math.sqrt(totalWeightByEventId.get(otherEventId));

                                double score = sharedMinWeightSum / (primaryNorm * otherNorm);

                                long smallerEventId = Math.min(primaryEventId, otherEventId);
                                long largerEventId = Math.max(primaryEventId, otherEventId);

                                calculatedSimilarities.add(EventSimilarityAvro.newBuilder()
                                        .setEventA(smallerEventId)
                                        .setEventB(largerEventId)
                                        .setScore(score)
                                        .setTimestamp(Instant.now())
                                        .build()
                                );
                            }
                        }
                    }
                }
            } else {
                calculatedSimilarities.addAll(newEventInteraction(userId, primaryEventId, actionWeightForPrimaryEvent));
            }
        }

        return calculatedSimilarities;
    }

    private List<EventSimilarityAvro> newEventInteraction(Long userId, Long primaryEventId, double actionWeightForPrimaryEvent) {
        log.info("New event interaction for userId: {}, eventId: {}", userId, primaryEventId);

        userWeightsByEventId.computeIfAbsent(primaryEventId, k ->
                new HashMap<>(Map.of(userId, actionWeightForPrimaryEvent))
        );
        totalWeightByEventId.put(primaryEventId, actionWeightForPrimaryEvent);

        List<EventSimilarityAvro> calculatedSimilarities = new ArrayList<>();

        for (Map.Entry<Long, Map<Long, Double>> entry : userWeightsByEventId.entrySet()) {
            long otherEventId = entry.getKey();

            if (primaryEventId != otherEventId) {
                Double userWeightForOtherEvent = entry.getValue().getOrDefault(userId, 0.0);

                if (userWeightForOtherEvent != 0.0) {
                    double sharedMinWeightSum = Math.min(actionWeightForPrimaryEvent, userWeightForOtherEvent);
                    sharedMinWeightsSumCache.put(primaryEventId, otherEventId, sharedMinWeightSum);

                    double primaryNorm = Math.sqrt(totalWeightByEventId.get(primaryEventId));
                    double otherNorm = Math.sqrt(totalWeightByEventId.get(otherEventId));
                    double score = sharedMinWeightSum / (primaryNorm * otherNorm);

                    long smallerEventId = Math.min(primaryEventId, otherEventId);
                    long largerEventId = Math.max(primaryEventId, otherEventId);

                    calculatedSimilarities.add(EventSimilarityAvro.newBuilder()
                            .setEventA(smallerEventId)
                            .setEventB(largerEventId)
                            .setScore(score)
                            .setTimestamp(Instant.now())
                            .build()
                    );
                }
            }
        }

        return calculatedSimilarities;
    }

    private double getWeight(ActionTypeAvro actionType) {
        return switch (actionType) {
            case VIEW -> 0.4;
            case REGISTER -> 0.8;
            case LIKE -> 1.0;
        };
    }

    static class SharedMinWeightSumCache {
        private final Map<Long, Map<Long, Double>> minWeightSumByEventPair = new HashMap<>();

        public void put(long eventA, long eventB, double sum) {
            long first = Math.min(eventA, eventB);
            long second = Math.max(eventA, eventB);

            minWeightSumByEventPair
                    .computeIfAbsent(first, e -> new HashMap<>())
                    .put(second, sum);
        }

        public double get(long eventA, long eventB) {
            long first = Math.min(eventA, eventB);
            long second = Math.max(eventA, eventB);

            return minWeightSumByEventPair
                    .computeIfAbsent(first, e -> new HashMap<>())
                    .getOrDefault(second, 0.0);
        }
    }
}
