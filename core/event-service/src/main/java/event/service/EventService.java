package event.service;

import dto.event.*;
import org.springframework.data.domain.Pageable;

import java.util.List;
import java.util.Set;

public interface EventService {
    List<EventShortDto> getEvents(Long userId, Pageable pageable);

    EventFullDto createEvent(Long userId, NewEventDto newEventDto);

    EventFullDto getEvent(Long userId, Long eventId, String ip);

    EventFullDto updateEvent(Long userId, Long eventId, UpdateEventUserRequest request);

    List<EventShortDto> getPublicEvents(PublicEventSearchRequest requestParams, Pageable pageable, String ip);

    EventFullDto getPublicEventById(Long eventId, String ip);

    EventFullDto getEventById(Long eventId);

    List<EventFullDto> getEvents(Set<Long> ids);
}
