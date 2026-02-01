package feign.event;

import dto.event.EventFullDto;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestParam;

import java.util.List;
import java.util.Map;

public interface EventOperations {
    @GetMapping("/{id}")
    EventFullDto getById(@PathVariable("id") Long id);

    @GetMapping("/confirmed")
    Map<Long, Long> getConfirmedRequestsBatchByEventIds(@RequestParam List<Long> eventIds);
}
