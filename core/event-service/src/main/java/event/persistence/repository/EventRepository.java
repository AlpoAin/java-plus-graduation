package event.persistence.repository;

import event.persistence.model.Event;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.domain.Specification;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.JpaSpecificationExecutor;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.lang.NonNull;

import java.util.List;
import java.util.Optional;

public interface EventRepository extends JpaRepository<Event, Long>, JpaSpecificationExecutor<Event> {

    List<Event> findAllById(@NonNull Iterable<Long> ids);

    boolean existsById(@NonNull Long id);

    Page<Event> findAllByInitiatorOrderByCreatedAtDesc(Long initiatorId, Pageable pageable);

    Optional<Event> findByIdAndInitiator(Long eventId, Long initiatorId);

    boolean existsByCategoryId(Long categoryId);

    Page<Event> findAll(@NonNull Specification<Event> spec, @NonNull Pageable pageable);

    @Query("SELECT e FROM Event e WHERE e.id = :eventId and e.initiator is not null")
    Optional<Event> findByIdWithInitiator(@Param("eventId") Long eventId);

    @Query("SELECT e FROM Event e " +
            "LEFT JOIN FETCH e.category " +
            "WHERE e.id = :eventId")
    Optional<Event> findByIdWithCategoryAndInitiator(@Param("eventId") Long eventId);

    @Query("SELECT e FROM Event e " +
            "LEFT JOIN FETCH e.category " +
            "WHERE e.id IN :eventIds")
    List<Event> findAllByIdWithCategoryAndInitiator(@Param("eventIds") List<Long> eventIds);

    Event findFirstByOrderByCreatedAtAsc();
}