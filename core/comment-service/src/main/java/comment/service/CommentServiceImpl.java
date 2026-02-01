package comment.service;

import comment.persistence.model.Comment;
import comment.persistence.mapper.CommentMapper;
import comment.persistence.repository.CommentRepository;
import dto.comment.CommentDto;
import dto.comment.NewCommentDto;
import dto.comment.UpdateCommentDto;
import dto.event.EventFullDto;
import dto.user.UserShortDto;
import feign.event.EventClient;
import feign.user.UserClient;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import util.exception.ConflictException;
import util.exception.NotFoundException;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
@Slf4j
public class CommentServiceImpl implements CommentService {

    private final CommentRepository commentRepository;
    private final CommentWriteService commentWriteService;
    private final EventClient eventClient;
    private final UserClient userClient;
    private final CommentMapper commentMapper;

    @Override
    public CommentDto createComment(Long userId, Long eventId, NewCommentDto newCommentDto) {
        log.debug("Создание нового комментария: userId={}, eventId={}", userId, eventId);

        EventFullDto event = eventClient.getById(eventId);

        if (event == null) {
            log.warn("Попытка создания комментария к несуществующему событию: eventId={}", eventId);
            throw new NotFoundException("Событие не найдено");
        }

        if (event.getState() == null) {
            throw new RuntimeException("Не удалось получить состояние публикации события");
        }

        if (!"PUBLISHED".equals(event.getState())) {
            log.warn("Попытка создания комментария к неопубликованному событию: eventId={}, state={}",
                    eventId, event.getState());
            throw new ConflictException("Невозможно прокомментировать неопубликованное событие");
        }

        Comment savedComment = commentWriteService.createComment(userId, eventId, newCommentDto);

        log.info("Создан новый комментарий: ID={}, authorId={}, eventId={}",
                savedComment.getId(), userId, eventId);

        return commentMapper.toDto(savedComment, userClient.getById(userId));
    }

    @Override
    public CommentDto updateComment(Long userId, Long commentId, UpdateCommentDto updateCommentDto) {
        log.debug("Обновление комментария: userId={}, commentId={}", userId, commentId);


        Comment updatedComment = commentWriteService.update(userId, commentId, updateCommentDto);
        log.info("Комментарий обновлен: commentId={}", commentId);

        return commentMapper.toDto(updatedComment, userClient.getById(userId));
    }

    @Override
    @Transactional
    public void deleteComment(Long userId, Long commentId) {
        log.debug("Удаление комментария пользователем: userId={}, commentId={}", userId, commentId);

        commentWriteService.deleteByUser(userId, commentId);

        log.info("Комментарий удален пользователем: commentId={}, userId={}", commentId, userId);
    }

    @Override
    @Transactional
    public void deleteCommentByAdmin(Long commentId) {
        log.debug("Удаление комментария администратором: commentId={}", commentId);

        commentWriteService.deleteByAdmin(commentId);

        log.info("Комментарий удален администратором: commentId={}", commentId);
    }

    @Override
    public List<CommentDto> getCommentsByEvent(Long eventId, Pageable pageable) {
        log.debug("Получение комментариев для события: eventId={}, page={}, size={}",
                eventId, pageable.getPageNumber(), pageable.getPageSize());

        if (eventClient.getById(eventId) == null) {
            log.warn("Попытка получения комментариев для несуществующего события: eventId={}", eventId);
            throw new NotFoundException("Событие не найдено");
        }
        List<Comment> comments = commentRepository.findByEventIdNotDeleted(eventId, pageable).getContent();
        Map<Long, UserShortDto> users = userClient.getByIds(comments.stream().map(Comment::getAuthor).distinct().toList());
        return comments.stream()
                .map(comment -> commentMapper.toDto(comment, users.get(comment.getAuthor())))
                .collect(Collectors.toList());
    }

    @Override
    public List<CommentDto> getCommentsByUser(Long userId, Pageable pageable) {
        log.debug("Получение комментариев пользователя: userId={}, page={}, size={}",
                userId, pageable.getPageNumber(), pageable.getPageSize());

        UserShortDto userDto = userClient.getById(userId);

        return commentRepository.findByAuthorIdAndIsDeletedFalse(userId, pageable)
                .stream()
                .map(comment -> commentMapper.toDto(comment, userDto))
                .collect(Collectors.toList());
    }

    @Override
    public CommentDto getCommentById(Long commentId) {
        log.debug("Получение комментария по ID: commentId={}", commentId);

        Comment comment = commentRepository.findByIdNotDeleted(commentId)
                .orElseThrow(() -> {
                    log.warn("Попытка получения несуществующего комментария: commentId={}", commentId);
                    return new NotFoundException("Комментарий не найден");
                });

        log.debug("Комментарий найден: commentId={}", commentId);
        return commentMapper.toDto(comment, userClient.getById(comment.getAuthor()));
    }

    @Override
    public List<CommentDto> getCommentsForEvents(List<Long> eventIds) {
        log.debug("Получение комментариев для списка событий: eventsCount={}", eventIds.size());

        List<Comment> comments = commentRepository.findByEventIds(eventIds);
        Map<Long, UserShortDto> users = userClient.getByIds(comments.stream().map(Comment::getAuthor).distinct().toList());
        List<CommentDto> result = comments.stream()
                .map(comment -> commentMapper.toDto(comment, users.get(comment.getAuthor())))
                .collect(Collectors.toList());

        log.debug("Найдено комментариев для {} событий: {}", eventIds.size(), result.size());
        return result;
    }
}