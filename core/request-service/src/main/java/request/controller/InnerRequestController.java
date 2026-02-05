package request.controller;

import dto.request.EventConfirmedRequestsDto;
import feign.request.RequestOperations;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;
import request.persistence.repository.ParticipationRequestRepository;

import java.util.List;

@RestController
@RequestMapping("/api/requests")
@RequiredArgsConstructor
public class InnerRequestController implements RequestOperations {
    private final ParticipationRequestRepository requestRepository;

    @GetMapping
    public List<EventConfirmedRequestsDto> getRequestsByEventIds(@RequestParam List<Long> eventIds) {
        return requestRepository.findConfirmedRequestsCountByEventIds(eventIds);
    }
}
