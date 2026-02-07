package feign.event;

import dto.event.EventFullDto;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;

@Component
public class EventClientFallback implements EventClient {
    @Override
    public EventFullDto getById(Long id) {
        return EventFullDto.builder().id(id).build();
    }

    @Override
    public Map<Long, Long> getConfirmedRequestsBatchByEventIds(List<Long> eventIds) {
        return Map.of();
    }
}
