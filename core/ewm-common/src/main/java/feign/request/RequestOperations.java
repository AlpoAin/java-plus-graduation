package feign.request;

import dto.request.EventConfirmedRequestsDto;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;

import java.util.List;

public interface RequestOperations {
    @GetMapping
    List<EventConfirmedRequestsDto> getRequestsByEventIds(@RequestParam List<Long> eventIds);
}
