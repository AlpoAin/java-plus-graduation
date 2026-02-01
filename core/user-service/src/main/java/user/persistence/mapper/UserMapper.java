package user.persistence.mapper;

import dto.user.NewUserRequest;
import dto.user.UserResponse;
import dto.user.UserShortDto;
import org.mapstruct.Mapper;
import org.mapstruct.ReportingPolicy;
import user.persistence.model.User;

@Mapper(componentModel = "spring", unmappedTargetPolicy = ReportingPolicy.IGNORE)
public interface UserMapper {
    User toEntity(NewUserRequest dto);

    UserResponse toDto(User user);

    UserShortDto toShortDto(User user);
}
