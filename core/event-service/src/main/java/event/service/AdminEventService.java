package event.service;

import dto.event.AdminEventSearchRequest;
import dto.event.EventFullDto;
import dto.event.UpdateEventAdminRequest;
import org.springframework.data.domain.Pageable;

import java.util.List;

public interface AdminEventService {
    List<EventFullDto> getEvents(AdminEventSearchRequest requestParams, Pageable pageable);

    EventFullDto updateEvent(Long eventId, UpdateEventAdminRequest request);
}
