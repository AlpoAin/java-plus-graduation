package client;

import com.google.protobuf.Timestamp;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.devh.boot.grpc.client.inject.GrpcClient;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.grpc.stats.collector.UserActionControllerGrpc;
import ru.yandex.practicum.grpc.stats.user.ActionTypeProto;
import ru.yandex.practicum.grpc.stats.user.UserActionProto;

import java.time.Instant;

@Slf4j
@Service
@RequiredArgsConstructor
public class CollectorGrpcClient {

    @GrpcClient("collector")
    private UserActionControllerGrpc.UserActionControllerBlockingStub userActionStub;

    private void sendUserAction(UserActionProto userActionRequest) {
        try {
            log.info("Sending user action proto: {}", userActionRequest);
            userActionStub.collectUserAction(userActionRequest);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new RuntimeException("Failed to send user action proto", e);
        }
    }

    public void sendViewAction(long userId, long eventId) {
        Instant now = Instant.now();

        sendUserAction(UserActionProto.newBuilder()
                .setUserId(userId)
                .setEventId(eventId)
                .setActionType(ActionTypeProto.ACTION_VIEW)
                .setTimestamp(Timestamp.newBuilder()
                        .setSeconds(now.getEpochSecond())
                        .setNanos(now.getNano())
                        .build())
                .build()
        );
    }

    public void sendLikeAction(long userId, long eventId) {
        Instant now = Instant.now();

        sendUserAction(UserActionProto.newBuilder()
                .setUserId(userId)
                .setEventId(eventId)
                .setActionType(ActionTypeProto.ACTION_LIKE)
                .setTimestamp(Timestamp.newBuilder()
                        .setSeconds(now.getEpochSecond())
                        .setNanos(now.getNano())
                        .build())
                .build()
        );
    }

    public void sendRegistrationAction(long userId, long eventId) {
        Instant now = Instant.now();

        sendUserAction(UserActionProto.newBuilder()
                .setUserId(userId)
                .setEventId(eventId)
                .setActionType(ActionTypeProto.ACTION_REGISTER)
                .setTimestamp(Timestamp.newBuilder()
                        .setSeconds(now.getEpochSecond())
                        .setNanos(now.getNano())
                        .build())
                .build()
        );
    }
}
