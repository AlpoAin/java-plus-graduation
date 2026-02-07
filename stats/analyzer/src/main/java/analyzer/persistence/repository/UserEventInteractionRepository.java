package analyzer.persistence.repository;

import analyzer.persistence.model.UserEventInteraction;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;

import java.util.List;

public interface UserEventInteractionRepository extends JpaRepository<UserEventInteraction, Long> {

    UserEventInteraction findByUserIdAndEventId(long userId, long eventId);

    @Query("""
        select
             i.eventId as eventId,
             sum(i.weight) as sum
        from UserEventInteraction i
             where i.eventId in :eventIds
        group by i.eventId
       """)
    List<EventWeightSumProjection> sumWeightsByEventIds(List<Long> eventIds);

    @Query("""
        select
             i.eventId
        from UserEventInteraction i
             where i.userId = :userId
        order by i.occurredAt desc
        limit :limit
        """)
    List<Long> findRecentEventIdsByUserId(Long userId, Integer limit);

    @Query("""
        select i
        from UserEventInteraction i
        where i.userId = :userId
          and i.eventId in :eventIds
        """)
    List<UserEventInteraction> findByUserIdAndEventIdIn(Long userId, List<Long> eventIds);
}
