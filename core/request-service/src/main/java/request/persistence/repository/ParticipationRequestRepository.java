package request.persistence.repository;

import dto.request.EventConfirmedRequestsDto;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import request.persistence.model.ParticipationRequest;

import java.util.List;
import java.util.Optional;

public interface ParticipationRequestRepository extends JpaRepository<ParticipationRequest, Long> {

    boolean existsByEventAndRequester(Long eventId, Long userId);

    @Query("SELECT NEW dto.request.EventConfirmedRequestsDto(" +
            "pr.event, COUNT(pr)) " +
            "FROM ParticipationRequest pr " +
            "WHERE pr.event IN :eventIds AND pr.status = enums.RequestStatus.CONFIRMED " +
            "GROUP BY pr.event")
    List<EventConfirmedRequestsDto> findConfirmedRequestsCountByEventIds(@Param("eventIds") List<Long> eventIds);

    @Query("SELECT pr FROM ParticipationRequest pr " +
            "WHERE pr.requester = :userId")
    List<ParticipationRequest> findAllByRequesterIdWithEventAndRequester(@Param("userId") Long userId);

    @Query("SELECT pr FROM ParticipationRequest pr " +
            "WHERE pr.event = :eventId")
    List<ParticipationRequest> findAllByEventIdWithEventAndRequester(@Param("eventId") Long eventId);

    @Query("SELECT pr FROM ParticipationRequest pr " +
            "WHERE pr.id IN :requestIds")
    List<ParticipationRequest> findAllByIdWithEventAndRequester(@Param("requestIds") List<Long> requestIds);

    @Query("SELECT pr FROM ParticipationRequest pr " +
            "WHERE pr.id = :requestId AND pr.requester = :userId")
    Optional<ParticipationRequest> findByIdWithEventAndRequester(@Param("requestId") Long requestId,
                                                                 @Param("userId") Long userId);
}