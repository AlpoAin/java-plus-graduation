package analyzer.controller;

import analyzer.service.AnalyzerService;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.devh.boot.grpc.server.service.GrpcService;
import ru.yandex.practicum.grpc.stats.analyzer.RecommendationsControllerGrpc;
import ru.yandex.practicum.grpc.stats.user.InteractionsCountRequestProto;
import ru.yandex.practicum.grpc.stats.user.RecommendedEventProto;
import ru.yandex.practicum.grpc.stats.user.SimilarEventsRequestProto;
import ru.yandex.practicum.grpc.stats.user.UserPredictionsRequestProto;

import java.util.Map;

@Slf4j
@GrpcService
@RequiredArgsConstructor
public class RecommendationsGrpcController extends RecommendationsControllerGrpc.RecommendationsControllerImplBase {
    private final AnalyzerService analyzerService;

    @Override
    public void getRecommendationsForUser(UserPredictionsRequestProto request,
                                          StreamObserver<RecommendedEventProto> responseObserver) {
        try {
            log.info("Received request for recommendations for user {}", request.getUserId());
            Map<Long, Double> recommendations = analyzerService.getRecommendations(request.getUserId(), request.getMaxResults());
            recommendations.forEach((eventId, score) -> responseObserver.onNext(
                    RecommendedEventProto.newBuilder()
                            .setEventId(eventId)
                            .setScore(score)
                            .build()
            ));
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(new StatusRuntimeException(
                    Status.INTERNAL.withDescription(e.getLocalizedMessage()).withCause(e)
            ));
        }
    }

    @Override
    public void getSimilarEvents(SimilarEventsRequestProto request,
                                 StreamObserver<RecommendedEventProto> responseObserver) {
        try {
            log.info("Received request for similar events for user {} and event {}", request.getUserId(), request.getEventId());
            Map<Long, Double> similarEvents = analyzerService.getSimilarEvents(
                    request.getEventId(), request.getUserId(), request.getMaxResults()
            );
            similarEvents.forEach((eventId, score) -> responseObserver.onNext(
                    RecommendedEventProto.newBuilder()
                            .setEventId(eventId)
                            .setScore(score)
                            .build()
            ));
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(new StatusRuntimeException(
                    Status.INTERNAL.withDescription(e.getLocalizedMessage()).withCause(e)
            ));
        }
    }

    @Override
    public void getInteractionsCount(InteractionsCountRequestProto request,
                                     StreamObserver<RecommendedEventProto> responseObserver) {
        try {
            log.info("Received request for interactions count for events {}", request.getEventIdList());
            Map<Long, Double> interactionSums = analyzerService.getInteractionsCount(request.getEventIdList());
            interactionSums.forEach((eventId, sum) -> responseObserver.onNext(
                    RecommendedEventProto.newBuilder()
                            .setEventId(eventId)
                            .setScore(sum)
                            .build()
            ));
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(new StatusRuntimeException(
                    Status.INTERNAL.withDescription(e.getLocalizedMessage()).withCause(e)
            ));
        }
    }
}
