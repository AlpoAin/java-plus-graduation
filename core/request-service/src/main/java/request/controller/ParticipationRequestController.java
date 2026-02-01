package request.controller;

import dto.request.EventRequestStatusUpdateRequest;
import dto.request.EventRequestStatusUpdateResult;
import dto.request.ParticipationRequestDto;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import request.service.ParticipationRequestService;

import java.util.List;

@RestController
@RequiredArgsConstructor
@Slf4j
public class ParticipationRequestController {
    private final ParticipationRequestService requestService;

    @GetMapping("/users/{userId}/requests")
    public List<ParticipationRequestDto> getUserRequest(@PathVariable Long userId) {
        log.info("GET/users/{}/requests", userId);
        return requestService.getUserRequests(userId);
    }

    @PostMapping("/users/{userId}/requests")
    @ResponseStatus(HttpStatus.CREATED)
    public ParticipationRequestDto createRequest(@PathVariable Long userId,
                                                 @RequestParam Long eventId) {
        log.info("POST/users/{}/request?eventId={}", userId, eventId);
        return requestService.createRequest(userId, eventId);
    }

    @PatchMapping("/users/{userId}/requests/{requestId}/cancel")
    public ParticipationRequestDto cancelRequest(
            @PathVariable Long userId,
            @PathVariable Long requestId) {
        log.info("PATCH /users/{}/requests/{}/cancel", userId, requestId);
        return requestService.cancelRequest(userId, requestId);
    }

    @GetMapping("/users/{userId}/events/{eventId}/requests")
    public List<ParticipationRequestDto> getRequestsByEvent(@PathVariable("userId") Long userId,
                                                            @PathVariable("eventId") Long eventId) {
        return requestService.getRequestsByEvent(userId, eventId);
    }

    @PatchMapping("/users/{userId}/events/{eventId}/requests")
    public EventRequestStatusUpdateResult changeRequestStatus(@PathVariable("userId") Long userId,
                                                              @PathVariable("eventId") Long eventId,
                                                              @RequestBody EventRequestStatusUpdateRequest request) {
        return requestService.changeRequestStatus(userId, eventId, request);
    }
}