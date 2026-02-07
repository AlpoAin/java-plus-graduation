package collector.service;

import ru.yandex.practicum.grpc.stats.user.UserActionProto;

public interface CollectorService {
    void publishUserAction(UserActionProto userActionProto);
}
