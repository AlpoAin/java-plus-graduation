package feign.user;

import org.springframework.cloud.openfeign.FeignClient;

@FeignClient(name = "user-service", path = "/api/users")
public interface UserClient extends UserOperations {
}
