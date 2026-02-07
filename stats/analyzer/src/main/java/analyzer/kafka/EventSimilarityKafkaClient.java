package analyzer.kafka;

import org.apache.kafka.clients.consumer.Consumer;
import ru.practicum.ewm.stats.avro.EventSimilarityAvro;

public interface EventSimilarityKafkaClient {
    Consumer<Void, EventSimilarityAvro> getConsumer();

    void stop();
}
