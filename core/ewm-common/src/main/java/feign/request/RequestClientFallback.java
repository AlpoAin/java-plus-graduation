package feign.request;

import dto.request.EventConfirmedRequestsDto;
import dto.request.ParticipationRequestDto;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class RequestClientFallback implements RequestClient {
    @Override
    public List<EventConfirmedRequestsDto> getRequestsByEventIds(List<Long> eventIds) {
        return List.of();
    }

    @Override
    public List<ParticipationRequestDto> getRequestsByUserId(Long userId) {
        return List.of();
    }
}
