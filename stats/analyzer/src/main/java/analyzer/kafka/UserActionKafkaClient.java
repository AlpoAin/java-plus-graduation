package analyzer.kafka;

import org.apache.kafka.clients.consumer.Consumer;
import ru.practicum.ewm.stats.avro.UserActionAvro;

public interface UserActionKafkaClient {
    Consumer<Void, UserActionAvro> getConsumer();

    void stop();
}
