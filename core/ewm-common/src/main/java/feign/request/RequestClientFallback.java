package feign.request;

import dto.request.EventConfirmedRequestsDto;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class RequestClientFallback implements feign.request.RequestClient {
    @Override
    public List<EventConfirmedRequestsDto> getRequestsByEventIds(List<Long> eventIds) {
        return List.of();
    }
}
