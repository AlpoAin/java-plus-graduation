package client;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.devh.boot.grpc.client.inject.GrpcClient;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.grpc.stats.analyzer.RecommendationsControllerGrpc;
import ru.yandex.practicum.grpc.stats.user.InteractionsCountRequestProto;
import ru.yandex.practicum.grpc.stats.user.RecommendedEventProto;
import ru.yandex.practicum.grpc.stats.user.SimilarEventsRequestProto;
import ru.yandex.practicum.grpc.stats.user.UserPredictionsRequestProto;

import java.util.Iterator;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

@Slf4j
@Service
@RequiredArgsConstructor
public class AnalyzerGrpcClient {

    @GrpcClient("analyzer")
    private RecommendationsControllerGrpc.RecommendationsControllerBlockingStub recommendationsStub;

    public Stream<RecommendedEventProto> streamUserRecommendations(UserPredictionsRequestProto request) {
        try {
            log.info("Getting recommendations for user: {}", request.getUserId());
            Iterator<RecommendedEventProto> iterator = recommendationsStub.getRecommendationsForUser(request);
            return toStream(iterator);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new RuntimeException("Failed to get recommendations for user", e);
        }
    }

    public Stream<RecommendedEventProto> streamSimilarEvents(SimilarEventsRequestProto request) {
        try {
            log.info("Getting similar events for event {} user {}", request.getEventId(), request.getUserId());
            Iterator<RecommendedEventProto> iterator = recommendationsStub.getSimilarEvents(request);
            return toStream(iterator);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new RuntimeException("Failed to get similar events", e);
        }
    }

    public Stream<RecommendedEventProto> streamInteractionsCount(InteractionsCountRequestProto request) {
        try {
            log.info("Getting interactions count for events {}", request.getEventIdList());
            Iterator<RecommendedEventProto> iterator = recommendationsStub.getInteractionsCount(request);
            return toStream(iterator);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new RuntimeException("Failed to get interactions count for events", e);
        }
    }

    private Stream<RecommendedEventProto> toStream(Iterator<RecommendedEventProto> iterator) {
        return StreamSupport.stream(
                Spliterators.spliteratorUnknownSize(iterator, Spliterator.ORDERED),
                false
        );
    }
}
