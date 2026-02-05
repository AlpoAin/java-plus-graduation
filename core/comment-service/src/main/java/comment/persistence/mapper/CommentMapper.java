package comment.persistence.mapper;


import comment.persistence.model.Comment;
import dto.comment.CommentDto;
import dto.comment.NewCommentDto;
import dto.comment.UpdateCommentDto;
import dto.user.UserShortDto;
import org.mapstruct.*;

@Mapper(componentModel = MappingConstants.ComponentModel.SPRING,
        unmappedTargetPolicy = ReportingPolicy.IGNORE)
public interface CommentMapper {

    @Mapping(target = "id", ignore = true)
    @Mapping(target = "createdDate", ignore = true)
    @Mapping(target = "updatedDate", ignore = true)
    @Mapping(target = "isEdited", ignore = true)
    @Mapping(target = "isDeleted", ignore = true)
    Comment toComment(NewCommentDto newCommentDto, Long author, Long event);

    @Mapping(target = "eventId", source = "comment.event")
    @Mapping(target = "id", source = "comment.id")
    @Mapping(target = "author", source = "user")
    CommentDto toDto(Comment comment, UserShortDto user);

    @BeanMapping(nullValuePropertyMappingStrategy = NullValuePropertyMappingStrategy.IGNORE)
    @Mapping(target = "id", ignore = true)
    @Mapping(target = "author", ignore = true)
    @Mapping(target = "event", ignore = true)
    @Mapping(target = "createdDate", ignore = true)
    @Mapping(target = "updatedDate", ignore = true)
    @Mapping(target = "isEdited", ignore = true)
    @Mapping(target = "isDeleted", ignore = true)
    void updateCommentFromDto(UpdateCommentDto dto, @MappingTarget Comment comment);
}