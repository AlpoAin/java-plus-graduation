package event.service;

import dto.event.*;
import org.springframework.data.domain.Pageable;

import java.util.List;
import java.util.Map;
import java.util.Set;

public interface EventService {
    List<EventShortDto> getEvents(Long userId, Pageable pageable);

    EventFullDto createEvent(Long userId, NewEventDto newEventDto);

    EventFullDto getEvent(Long userId, Long eventId);

    EventFullDto updateEvent(Long userId, Long eventId, UpdateEventUserRequest request);

    List<EventShortDto> getPublicEvents(PublicEventSearchRequest requestParams, Pageable pageable);

    EventFullDto getPublicEventById(Long eventId, Long userId);

    EventFullDto getEventById(Long eventId);

    List<EventFullDto> getEvents(Set<Long> ids, Map<Long, Double> ratings);

    List<EventFullDto> getRecommendations(Long userId, Integer limit);

    void setLike(Long eventId, Long userId);
}
