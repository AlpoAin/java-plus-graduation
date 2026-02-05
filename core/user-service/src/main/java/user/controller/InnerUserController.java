package user.controller;

import dto.user.UserShortDto;
import feign.user.UserOperations;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;
import user.service.UserService;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/users")
@RequiredArgsConstructor
public class InnerUserController implements UserOperations {
    private final UserService userService;

    @GetMapping("/{id}")
    public UserShortDto getById(@PathVariable Long id) {
        return userService.getUserById(id);
    }

    @Override
    public Map<Long, UserShortDto> getByIds(@RequestParam List<Long> ids) {
        return userService.getUsers(ids);
    }
}