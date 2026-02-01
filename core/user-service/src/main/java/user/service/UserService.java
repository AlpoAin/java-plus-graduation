package user.service;

import dto.user.NewUserRequest;
import dto.user.UserResponse;
import dto.user.UserShortDto;
import org.springframework.data.domain.Pageable;
import user.persistence.model.User;

import java.util.List;
import java.util.Map;

public interface UserService {
    UserResponse createUser(NewUserRequest userRequest);

    List<UserResponse> getUsers(List<Long> ids, Pageable pageable);

    Map<Long, UserShortDto> getUsers(List<Long> ids);

    UserShortDto getUserById(Long userId);

    void deleteUser(Long userId);

    void checkUserExists(Long userId);

    User getUserEntityById(Long userId);
}