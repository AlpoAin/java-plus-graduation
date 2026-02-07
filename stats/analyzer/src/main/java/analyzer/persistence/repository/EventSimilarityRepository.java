package analyzer.persistence.repository;

import analyzer.persistence.model.EventSimilarity;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;

import java.util.List;

public interface EventSimilarityRepository extends JpaRepository<EventSimilarity, Long> {

    EventSimilarity findByEventAIdAndEventBId(Long eventAId, Long eventBId);

    @Query("""
        select
             s
        from EventSimilarity s
        where (s.eventAId = :eventId and s.eventBId not in :interactedIds)
             or (s.eventBId = :eventId and s.eventAId not in :interactedIds)
        order by s.score desc
        """)
    List<EventSimilarity> findSimilarNotInteracted(List<Long> interactedIds, Long eventId, Pageable pageable);

    @Query("""
        select s
        from EventSimilarity s
        where (s.eventAId in :interactedIds and s.eventBId not in :interactedIds)
             or (s.eventBId in :interactedIds and s.eventAId not in :interactedIds)
        order by s.score desc
        """)
    List<EventSimilarity> findCandidatesNotInteracted(List<Long> interactedIds, Pageable pageable);

    @Query("""
        select s
        from EventSimilarity s
        where (s.eventAId = :candidateId and s.eventBId in :interactedIds)
           or (s.eventBId = :candidateId and s.eventAId in :interactedIds)
        order by s.score desc
        """)
    List<EventSimilarity> findSimilarToInteractedEvents(Long candidateId, List<Long> interactedIds, Pageable pageable);
}
