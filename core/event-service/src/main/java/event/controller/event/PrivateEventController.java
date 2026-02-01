package event.controller.event;

import dto.event.EventFullDto;
import dto.event.EventShortDto;
import dto.event.NewEventDto;
import dto.event.UpdateEventUserRequest;
import event.service.EventService;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Positive;
import jakarta.validation.constraints.PositiveOrZero;
import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.http.HttpStatus;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/users")
@RequiredArgsConstructor
@Validated
public class PrivateEventController {
    private final EventService eventService;

    @GetMapping("/{userId}/events")
    public List<EventShortDto> getEvents(@PathVariable("userId") Long userId,
                                         @RequestParam(defaultValue = "0") @PositiveOrZero Integer from,
                                         @RequestParam(defaultValue = "10") @Positive Integer size) {
        Pageable pageable = PageRequest.of(from / size, size);
        return eventService.getEvents(userId, pageable);
    }

    @PostMapping("/{userId}/events")
    @ResponseStatus(HttpStatus.CREATED)
    public EventFullDto createEvent(@PathVariable("userId") Long userId,
                                    @Valid @RequestBody NewEventDto newEventDto) {
        return eventService.createEvent(userId, newEventDto);
    }

    @GetMapping("/{userId}/events/{eventId}")
    public EventFullDto getEvent(@PathVariable("userId") Long userId,
                                 @PathVariable("eventId") Long eventId,
                                 HttpServletRequest request) {
        return eventService.getEvent(userId, eventId, request.getRemoteAddr());
    }

    @PatchMapping("/{userId}/events/{eventId}")
    public EventFullDto updateEvent(@PathVariable("userId") Long userId,
                                    @PathVariable("eventId") Long eventId,
                                    @Valid @RequestBody UpdateEventUserRequest request) {
        return eventService.updateEvent(userId, eventId, request);
    }
}
