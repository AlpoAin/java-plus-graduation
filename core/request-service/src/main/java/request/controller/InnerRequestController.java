package request.controller;

import dto.request.EventConfirmedRequestsDto;
import dto.request.ParticipationRequestDto;
import feign.request.RequestOperations;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;
import request.persistence.repository.ParticipationRequestRepository;
import request.service.ParticipationRequestService;

import java.util.List;

@RestController
@RequestMapping("/api/requests")
@RequiredArgsConstructor
public class InnerRequestController implements RequestOperations {
    private final ParticipationRequestRepository requestRepository;
    private final ParticipationRequestService service;

    @GetMapping
    public List<EventConfirmedRequestsDto> getRequestsByEventIds(@RequestParam List<Long> eventIds) {
        return requestRepository.findConfirmedRequestsCountByEventIds(eventIds);
    }

    @GetMapping("/by-user")
    public List<ParticipationRequestDto> getRequestsByUserId(@RequestParam Long userId) {
        return service.getUserRequests(userId);
    }
}
