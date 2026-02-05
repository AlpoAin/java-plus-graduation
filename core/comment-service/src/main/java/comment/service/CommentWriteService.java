package comment.service;

import comment.persistence.model.Comment;
import comment.persistence.mapper.CommentMapper;
import comment.persistence.repository.CommentRepository;
import dto.comment.NewCommentDto;
import dto.comment.UpdateCommentDto;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import util.exception.ConflictException;
import util.exception.NotFoundException;

@Service
@RequiredArgsConstructor
public class CommentWriteService {
    private final CommentRepository commentRepository;
    private final CommentMapper commentMapper;

    @Transactional
    public Comment createComment(Long userId, Long eventId, NewCommentDto dto) {
        Comment comment = commentMapper.toComment(dto, userId, eventId);
        return commentRepository.save(comment);
    }

    @Transactional
    public Comment update(Long userId, Long commentId, UpdateCommentDto dto) {
        Comment comment = commentRepository.findByIdAndAuthorId(commentId, userId)
                .orElseThrow(() -> new NotFoundException("Комментарий не найден"));

        if (comment.getIsDeleted()) {
            throw new ConflictException("Не удается обновить удаленный комментарий");
        }

        commentMapper.updateCommentFromDto(dto, comment);
        comment.setIsEdited(true);

        return commentRepository.save(comment);
    }

    @Transactional
    public void deleteByUser(Long userId, Long commentId) {
        Comment comment = commentRepository.findByIdAndAuthorId(commentId, userId)
                .orElseThrow(() -> new NotFoundException("Комментарий не найден"));

        comment.setIsDeleted(true);
        commentRepository.save(comment);
    }

    @Transactional
    public void deleteByAdmin(Long commentId) {
        Comment comment = commentRepository.findById(commentId)
                .orElseThrow(() -> new NotFoundException("Комментарий не найден"));

        comment.setIsDeleted(true);
        commentRepository.save(comment);
    }

}
