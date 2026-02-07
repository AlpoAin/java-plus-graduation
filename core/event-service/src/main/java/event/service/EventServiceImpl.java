package event.service;

import client.AnalyzerGrpcClient;
import client.CollectorGrpcClient;
import dto.event.*;
import dto.request.ParticipationRequestDto;
import dto.user.UserShortDto;
import enums.RequestStatus;
import event.persistence.model.Event;
import event.persistence.model.EventState;
import enums.StateAction;
import event.persistence.mapper.EventMapper;
import event.persistence.repository.EventRepository;
import event.persistence.repository.specification.EventSpecifications;
import feign.request.RequestClient;
import feign.user.UserClient;
import jakarta.validation.ValidationException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.jpa.domain.Specification;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.grpc.stats.user.RecommendedEventProto;
import ru.yandex.practicum.grpc.stats.user.UserPredictionsRequestProto;
import util.exception.NotFoundException;
import event.validation.EventValidationUtils;

import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Collectors;

@Slf4j
@Service
@RequiredArgsConstructor
@Transactional
public class EventServiceImpl implements EventService {
    private final EventRepository eventRepository;
    private final EventMapper eventMapper;
    private final UserClient userClient;
    private final EventStatsService eventStatsService;
    private final CollectorGrpcClient  collectorGrpcClient;
    private final AnalyzerGrpcClient  analyzerGrpcClient;
    private final RequestClient requestClient;

    @Override
    @Transactional(readOnly = true)
    public List<EventShortDto> getEvents(Long userId, Pageable pageable) {
        userClient.getById(userId);
        Page<Event> eventsPage = eventRepository.findAllByInitiatorOrderByCreatedAtDesc(userId, pageable);

        return eventStatsService.enrichEventsShortDtoBatch(eventsPage.getContent(), eventMapper);
    }

    @Override
    @Transactional(readOnly = true)
    public EventFullDto getEvent(Long userId, Long eventId) {
        userClient.getById(userId);
        Event event = eventRepository.findByIdAndInitiator(eventId, userId)
                .orElseThrow(() -> new NotFoundException("Event not found"));

        return eventStatsService.enrichEventFullDto(event, eventMapper);
    }

    @Override
    @Transactional
    public EventFullDto createEvent(Long userId, NewEventDto newEventDto) {
        EventValidationUtils.validateEventDate(newEventDto.getEventDate(), 2);

        Event event = eventMapper.toEvent(newEventDto);

        event.setInitiator(userId);
        event.setCreatedAt(LocalDateTime.now());
        event.setState(EventState.PENDING.toString());

        Event savedEvent = eventRepository.save(event);

        UserShortDto user = userClient.getById(userId);

        EventFullDto eventDto = eventMapper.toFullDto(savedEvent, user);
        eventDto.setConfirmedRequests(0L);
        eventDto.setRating(0.0);
        return eventDto;
    }

    @Override
    @Transactional
    public EventFullDto updateEvent(Long userId, Long eventId, UpdateEventUserRequest request) {
        userClient.getById(userId);
        Event event = eventRepository.findByIdAndInitiator(eventId, userId)
                .orElseThrow(() -> new NotFoundException("Event not found"));

        EventValidationUtils.validateEventStateForUpdate(event);
        EventValidationUtils.validateEventDate(request.getEventDate(), 2);
        EventValidationUtils.validateParticipantLimit(request.getParticipantLimit());

        eventMapper.updateEventFromUserRequest(request, event);
        updateEventState(event, request);

        Event updatedEvent = eventRepository.save(event);
        return eventStatsService.enrichEventFullDto(updatedEvent, eventMapper);
    }

    @Override
    @Transactional(readOnly = true)
    public List<EventShortDto> getPublicEvents(PublicEventSearchRequest requestParams, Pageable pageable) {

        EventValidationUtils.validateDateRange(requestParams.getRangeStart(), requestParams.getRangeEnd());

        Specification<Event> spec = buildPublicEventsSpecification(requestParams);

        List<Event> events = eventRepository.findAll(spec, pageable).getContent();
        List<EventShortDto> result = eventStatsService.enrichEventsShortDtoBatch(events, eventMapper);

        return sortEvents(result, requestParams.getSort());
    }

    @Override
    @Transactional(readOnly = true)
    public EventFullDto getPublicEventById(Long eventId, Long userId) {
        Event event = eventRepository.findById(eventId)
                .filter(e -> EventState.PUBLISHED.toString().equals(e.getState()))
                .orElseThrow(() -> new NotFoundException("Событие с id=" + eventId + " не найдено"));

        try {
            log.info("Sending user {} view for event {}", userId, eventId);
            collectorGrpcClient.sendViewAction(userId, eventId);
        } catch (RuntimeException e) {
            log.error("Sending user action failed", e);
        }

        return eventStatsService.enrichEventFullDto(event, eventMapper);
    }

    @Override
    public EventFullDto getEventById(Long eventId) {
        Event event = eventRepository.findById(eventId)
                .orElseThrow(() -> new NotFoundException("Event not found"));

        return eventStatsService.enrichEventFullDto(event, eventMapper);
    }

    @Override
    public List<EventFullDto> getEvents(Set<Long> ids, Map<Long, Double> ratings) {
        return eventStatsService.enrichEventsFullDto(ids, eventMapper, ratings);
    }

    @Override
    public List<EventFullDto> getRecommendations(Long userId, Integer limit) {
        Map<Long, Double> recs = new LinkedHashMap<>();
        try {
            log.info("Getting recommendations for user {}", userId);
            recs = analyzerGrpcClient.streamUserRecommendations(UserPredictionsRequestProto.newBuilder()
                            .setUserId(userId)
                            .setMaxResults(limit)
                            .build()
                    )
                    .collect(Collectors.toMap(
                                    RecommendedEventProto::getEventId,
                                    RecommendedEventProto::getScore
                            )
                    );
        } catch (RuntimeException e) {
            log.error("Getting recommendations failed", e);
        }

        if (!recs.isEmpty()) {
            return getEvents(recs.keySet(), recs);
        }

        return List.of();
    }

    @Override
    public void setLike(Long eventId, Long userId) {
        List<ParticipationRequestDto> userRequests = requestClient.getRequestsByUserId(userId);
        if (userRequests.isEmpty()) {
            log.info("User {} has no requests for event {}", userId, eventId);
            throw new ValidationException("Ставить лайки можно только на посещенные мероприятия");
        }

        boolean hasConfirmedVisit = userRequests.stream()
                .anyMatch(req ->
                        req.getEvent().equals(eventId) && req.getStatus().equals(RequestStatus.CONFIRMED.name())
                );

        if (!hasConfirmedVisit) {
            log.info("User {} has no confirmed requests for event {}", userId, eventId);
            throw new ValidationException("Ставить лайки можно только на посещенные мероприятия");
        }

        try {
            log.info("Sending like from user {} for event {}", userId, eventId);
            collectorGrpcClient.sendLikeAction(userId, eventId);
        } catch (RuntimeException e) {
            log.error("Sending like failed", e);
        }
    }

    private Specification<Event> buildPublicEventsSpecification(PublicEventSearchRequest params) {
        Specification<Event> spec = Specification.where(EventSpecifications.isPublished());

        if (params.getText() != null && !params.getText().trim().isEmpty()) {
            spec = spec.and(EventSpecifications.containsText(params.getText()));
        }

        if (params.getCategories() != null && !params.getCategories().isEmpty()) {
            spec = spec.and(EventSpecifications.hasCategories(params.getCategories()));
        }

        if (params.getPaid() != null) {
            spec = spec.and(EventSpecifications.isPaid(params.getPaid()));
        }

        LocalDateTime actualRangeStart = (params.getRangeStart() == null && params.getRangeEnd() == null) ?
                LocalDateTime.now() : params.getRangeStart();

        if (actualRangeStart != null) {
            spec = spec.and(EventSpecifications.startsAfter(actualRangeStart));
        }

        if (params.getRangeEnd() != null) {
            spec = spec.and(EventSpecifications.endsBefore(params.getRangeEnd()));
        }

        if (Boolean.TRUE.equals(params.getOnlyAvailable())) {
            spec = spec.and(EventSpecifications.hasAvailableSlots());
        }

        return spec;
    }

    private Pageable createPageable(Integer from, Integer size, String sort) {
        int pageSize = size != null ? Math.max(1, size) : 10;
        int pageFrom = from != null ? Math.max(0, from) : 0;
        int page = pageFrom / pageSize;

        Sort sorting = "VIEWS".equals(sort) ?
                Sort.by(Sort.Direction.DESC, "id") :
                Sort.by(Sort.Direction.DESC, "eventDate");

        return PageRequest.of(page, pageSize, sorting);
    }

    private List<EventShortDto> sortEvents(List<EventShortDto> events, String sort) {
        if ("VIEWS".equals(sort)) {
            return events.stream()
                    .sorted(Comparator.comparing(EventShortDto::getRating).reversed())
                    .collect(Collectors.toList());
        }
        return events;
    }

    private void updateEventState(Event event, UpdateEventUserRequest request) {
        if (request.getStateAction() != null) {
            StateAction stateAction = StateAction.valueOf(request.getStateAction());
            if (stateAction == StateAction.SEND_TO_REVIEW) {
                event.setState(EventState.PENDING.toString());
            } else if (stateAction == StateAction.CANCEL_REVIEW) {
                event.setState(EventState.CANCELED.toString());
            }
        }
    }
}