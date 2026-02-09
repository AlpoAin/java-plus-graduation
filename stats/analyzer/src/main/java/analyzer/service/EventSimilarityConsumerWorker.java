package analyzer.service;

import analyzer.kafka.EventSimilarityKafkaClient;
import config.KafkaClientProperties;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.springframework.stereotype.Component;
import ru.practicum.ewm.stats.avro.EventSimilarityAvro;

import java.util.HashMap;
import java.util.Map;

@Slf4j
@Component
@RequiredArgsConstructor
public class EventSimilarityConsumerWorker implements Runnable {
    private final KafkaClientProperties kafkaClientProperties;
    private final AnalyzerService analyzerService;
    private final EventSimilarityKafkaClient kafkaClient;

    private static final Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();

    @Override
    public void run() {
        Runtime.getRuntime().addShutdownHook(new Thread(kafkaClient.getConsumer()::wakeup));
        try {
            while (true) {
                log.info("Getting similarities...");
                processRecords(kafkaClient.getConsumer().poll(kafkaClientProperties.getConsumer().getPollTimeoutMs()));
            }
        } catch (WakeupException ignored) {

        } catch (Exception e) {
            log.error("Error during process topic {}", kafkaClientProperties.getTopics().getEventSimilarityTopic(), e);
        } finally {
            try {
                kafkaClient.getConsumer().commitSync(currentOffsets);
            } finally {
                log.info("Closing analyzer events-similarity consumer...");
                kafkaClient.stop();
            }
        }
    }

    private void processRecords(ConsumerRecords<Void, EventSimilarityAvro> records) {
        for (ConsumerRecord<Void, EventSimilarityAvro> record : records) {
            log.info("Processing record - topic:[{}] partition:[{}] offset:[{}] value: {}",
                    record.topic(), record.partition(), record.offset(), record.value());
            analyzerService.processEventSimilarity(record.value());
            kafkaClient.getConsumer().commitSync();
        }
    }
}
