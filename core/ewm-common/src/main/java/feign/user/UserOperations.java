package feign.user;

import dto.user.UserShortDto;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestParam;

import java.util.List;
import java.util.Map;

public interface UserOperations {
    @GetMapping("/{id}")
    UserShortDto getById(@PathVariable("id") Long id);

    @GetMapping
    Map<Long, UserShortDto> getByIds(@RequestParam List<Long> ids);
}
