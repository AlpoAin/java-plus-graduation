package comment.controller;

import comment.service.CommentService;
import dto.comment.CommentDto;
import jakarta.validation.constraints.Positive;
import jakarta.validation.constraints.PositiveOrZero;
import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.PageRequest;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/comments")
@RequiredArgsConstructor
public class PublicCommentController {

    private final CommentService commentService;

    @GetMapping("/event/{eventId}")
    public List<CommentDto> getCommentsByEvent(
            @PathVariable Long eventId,
            @RequestParam(defaultValue = "0") @PositiveOrZero Integer from,
            @RequestParam(defaultValue = "10") @Positive Integer size) {

        int page = from / size;
        return commentService.getCommentsByEvent(eventId, PageRequest.of(page, size));
    }

    @GetMapping("/{commentId}")
    public CommentDto getComment(@PathVariable Long commentId) {
        return commentService.getCommentById(commentId);
    }
}
