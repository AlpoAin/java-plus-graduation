package feign.request;

import org.springframework.cloud.openfeign.FeignClient;

@FeignClient(
        name = "request-service",
        path = "/api/requests",
        fallback = RequestClientFallback.class
)
public interface RequestClient extends RequestOperations {
}
