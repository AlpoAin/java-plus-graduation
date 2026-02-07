package event.service;

import client.AnalyzerGrpcClient;
import dto.event.EventFullDto;
import dto.event.EventShortDto;
import dto.request.EventConfirmedRequestsDto;
import dto.user.UserShortDto;
import event.persistence.model.Event;
import event.persistence.mapper.EventMapper;
import event.persistence.repository.EventRepository;
import feign.request.RequestClient;
import feign.user.UserClient;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.grpc.stats.user.InteractionsCountRequestProto;
import ru.yandex.practicum.grpc.stats.user.RecommendedEventProto;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
@Slf4j
public class EventStatsService {
    private final RequestClient requestClient;
    private final UserClient userClient;
    private final EventRepository eventRepository;
    private final AnalyzerGrpcClient analyzerGrpcClient;

    public Map<Long, Double> getRatingsForEvents(List<Long> eventIds) {
        Map<Long, Double> ratings = new LinkedHashMap<>();
        if (eventIds.isEmpty()) {
            return Map.of();
        }

        try {
            log.info("Getting ratings for eventIds {}", eventIds);
            ratings = analyzerGrpcClient.streamInteractionsCount(InteractionsCountRequestProto.newBuilder()
                    .addAllEventId(eventIds)
                    .build()
            ).collect(Collectors.toMap(RecommendedEventProto::getEventId, RecommendedEventProto::getScore));
        } catch (Exception e) {
            log.error("Getting ratings for eventIds failed", e);
        }

        return ratings;
    }

    public Map<Long, Long> getConfirmedRequestsBatch(List<Long> eventIds) {
        if (eventIds.isEmpty()) {
            return Map.of();
        }

        List<EventConfirmedRequestsDto> results = requestClient.getRequestsByEventIds(eventIds);

        Map<Long, Long> confirmedRequestsMap = eventIds.stream()
                .collect(Collectors.toMap(id -> id, id -> 0L));

        results.forEach(dto -> confirmedRequestsMap.put(dto.getEventId(), dto.getConfirmedCount()));

        log.debug("Получены подтвержденные запросы для {} событий", results.size());
        return confirmedRequestsMap;
    }

    public EventFullDto enrichEventFullDto(Event event, EventMapper eventMapper) {
        UserShortDto user = userClient.getById(event.getInitiator());
        EventFullDto dto = eventMapper.toFullDto(event, user);
        Map<Long, Long> confirmedRequests = getConfirmedRequestsBatch(List.of(event.getId()));
        Map<Long, Double> ratings = getRatingsForEvents(List.of(event.getId()));

        dto.setConfirmedRequests(confirmedRequests.getOrDefault(event.getId(), 0L));
        dto.setRating(ratings.getOrDefault(event.getId(), 0.0));
        return dto;
    }

    public List<EventFullDto> enrichEventsFullDto(Iterable<Long> ids, EventMapper eventMapper, Map<Long, Double> ratings) {
        List<Event> events = eventRepository.findAllById(ids);
        Map<Long, UserShortDto> users = userClient.getByIds(
                events.stream().map(Event::getInitiator).toList()
        );

        Map<Long, Long> confirmedRequests = getConfirmedRequestsBatch((List<Long>) ids);
        if (ratings.isEmpty()) {
            ratings = getRatingsForEvents((List<Long>) ids);
        }

        Map<Long, Double> finalRatings = ratings;
        return events.stream()
                .map(event -> {
                    EventFullDto dto = eventMapper.toFullDto(event, users.get(event.getInitiator()));
                    dto.setRating(finalRatings.getOrDefault(event.getId(), 0.0));
                    dto.setConfirmedRequests(confirmedRequests.getOrDefault(event.getId(), 0L));
                    return dto;
                })
                .toList();
    }

    public EventShortDto enrichEventShortDto(Event event, EventMapper eventMapper) {
        UserShortDto user = userClient.getById(event.getInitiator());
        EventShortDto dto = eventMapper.toShortDto(event, user);
        Map<Long, Long> confirmedRequests = getConfirmedRequestsBatch(List.of(event.getId()));
        Map<Long, Double> ratings = getRatingsForEvents(List.of(event.getId()));

        dto.setConfirmedRequests(confirmedRequests.getOrDefault(event.getId(), 0L));
        dto.setRating(ratings.getOrDefault(event.getId(), 0.0));
        return dto;
    }


    public List<EventFullDto> enrichEventsFullDtoBatch(List<Event> events, EventMapper eventMapper) {
        if (events.isEmpty()) {
            return List.of();
        }

        List<Long> eventIds = events.stream()
                .map(Event::getId)
                .collect(Collectors.toList());

        Map<Long, Long> confirmedRequestsMap = getConfirmedRequestsBatch(eventIds);
        Map<Long, Double> ratings = getRatingsForEvents(eventIds);

        return events.stream()
                .map(event -> {
                    UserShortDto user = userClient.getById(event.getInitiator());
                    EventFullDto dto = eventMapper.toFullDto(event, user);
                    dto.setConfirmedRequests(confirmedRequestsMap.getOrDefault(event.getId(), 0L));
                    dto.setRating(ratings.getOrDefault(event.getId(), 0.0));
                    return dto;
                })
                .collect(Collectors.toList());
    }


    public List<EventShortDto> enrichEventsShortDtoBatch(List<Event> events, EventMapper eventMapper) {
        if (events.isEmpty()) {
            return List.of();
        }

        List<Long> eventIds = events.stream()
                .map(Event::getId)
                .collect(Collectors.toList());

        Map<Long, Long> confirmedRequestsMap = getConfirmedRequestsBatch(eventIds);
        Map<Long, Double> ratings = getRatingsForEvents(eventIds);

        return events.stream()
                .map(event -> {
                    UserShortDto user = userClient.getById(event.getInitiator());
                    EventShortDto dto = eventMapper.toShortDto(event, user);
                    dto.setConfirmedRequests(confirmedRequestsMap.getOrDefault(event.getId(), 0L));
                    dto.setRating(ratings.getOrDefault(event.getId(), 0.0));
                    return dto;
                })
                .collect(Collectors.toList());
    }
}