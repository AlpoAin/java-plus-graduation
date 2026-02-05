package event.service;

import dto.event.*;
import dto.user.UserShortDto;
import event.persistence.model.Event;
import event.persistence.model.EventState;
import enums.StateAction;
import event.persistence.mapper.EventMapper;
import event.persistence.repository.EventRepository;
import event.persistence.repository.specification.EventSpecifications;
import feign.user.UserClient;
import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.jpa.domain.Specification;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import util.exception.NotFoundException;
import event.validation.EventValidationUtils;

import java.time.LocalDateTime;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
@Transactional
public class EventServiceImpl implements EventService {
    private static final String ENDPOINT = "/events";

    private final EventRepository eventRepository;
    private final EventMapper eventMapper;
    private final UserClient userClient;
    private final EventStatsService eventStatsService;

    @Override
    @Transactional(readOnly = true)
    public List<EventShortDto> getEvents(Long userId, Pageable pageable) {
        userClient.getById(userId);
        Page<Event> eventsPage = eventRepository.findAllByInitiatorOrderByCreatedAtDesc(userId, pageable);

        return eventStatsService.enrichEventsShortDtoBatch(eventsPage.getContent(), eventMapper);
    }

    @Override
    @Transactional(readOnly = true)
    public EventFullDto getEvent(Long userId, Long eventId, String ip) {
        userClient.getById(userId);
        Event event = eventRepository.findByIdAndInitiator(eventId, userId)
                .orElseThrow(() -> new NotFoundException("Event not found"));

        eventStatsService.recordHit(ENDPOINT + "/" + eventId, ip);
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
        eventDto.setViews(0L);
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
    public List<EventShortDto> getPublicEvents(PublicEventSearchRequest requestParams, Pageable pageable, String ip) {

        EventValidationUtils.validateDateRange(requestParams.getRangeStart(), requestParams.getRangeEnd());

        Specification<Event> spec = buildPublicEventsSpecification(requestParams);

        List<Event> events = eventRepository.findAll(spec, pageable).getContent();
        List<EventShortDto> result = eventStatsService.enrichEventsShortDtoBatch(events, eventMapper);

        eventStatsService.recordHit(ENDPOINT, ip);

        return sortEvents(result, requestParams.getSort());
    }

    @Override
    @Transactional(readOnly = true)
    public EventFullDto getPublicEventById(Long eventId, String ip) {
        Event event = eventRepository.findById(eventId)
                .filter(e -> EventState.PUBLISHED.toString().equals(e.getState()))
                .orElseThrow(() -> new NotFoundException("Событие с id=" + eventId + " не найдено"));

        eventStatsService.recordHit(ENDPOINT + "/" + eventId, ip);
        return eventStatsService.enrichEventFullDto(event, eventMapper);
    }

    @Override
    public EventFullDto getEventById(Long eventId) {
        Event event = eventRepository.findById(eventId)
                .orElseThrow(() -> new NotFoundException("Event not found"));

        return eventStatsService.enrichEventFullDto(event, eventMapper);
    }

    @Override
    public List<EventFullDto> getEvents(Set<Long> ids) {
        return eventStatsService.enrichEventsFullDto(ids, eventMapper);
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
                    .sorted(Comparator.comparing(EventShortDto::getViews).reversed())
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