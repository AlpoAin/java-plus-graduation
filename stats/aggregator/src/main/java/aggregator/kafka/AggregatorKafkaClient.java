package aggregator.kafka;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.Producer;
import ru.practicum.ewm.stats.avro.EventSimilarityAvro;
import ru.practicum.ewm.stats.avro.UserActionAvro;

public interface AggregatorKafkaClient {
    Producer<Void, EventSimilarityAvro> getProducer();

    Consumer<Void, UserActionAvro> getConsumer();

    void stop();
}