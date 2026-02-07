package collector.kafka;

import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.clients.producer.Producer;

public interface CollectorProducerClient {
    Producer<Void, SpecificRecordBase> getProducer();

    void stop();
}
