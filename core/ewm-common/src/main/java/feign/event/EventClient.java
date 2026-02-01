package feign.event;

import org.springframework.cloud.openfeign.FeignClient;

@FeignClient(
        name = "event-service",
        path = "/api/events",
        fallback = EventClientFallback.class
)
public interface EventClient extends EventOperations {
}
