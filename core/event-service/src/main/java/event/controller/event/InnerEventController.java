package event.controller.event;

import dto.event.EventFullDto;
import event.service.EventService;
import event.service.EventStatsService;
import feign.event.EventOperations;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/events")
@RequiredArgsConstructor
public class InnerEventController implements EventOperations {
    private final EventService eventService;
    private final EventStatsService eventStatsService;

    @Override
    @GetMapping("/{id}")
    public EventFullDto getById(@PathVariable("id") Long id) {
        return eventService.getEventById(id);
    }

    @Override
    @GetMapping("/confirmed")
    public Map<Long, Long> getConfirmedRequestsBatchByEventIds(@RequestParam List<Long> eventIds) {
        return eventStatsService.getConfirmedRequestsBatch(eventIds);
    }
}
