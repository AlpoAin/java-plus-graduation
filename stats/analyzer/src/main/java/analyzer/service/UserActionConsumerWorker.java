package analyzer.service;

import analyzer.kafka.UserActionKafkaClient;
import config.KafkaClientProperties;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.springframework.stereotype.Component;
import ru.practicum.ewm.stats.avro.UserActionAvro;

import java.util.HashMap;
import java.util.Map;

@Slf4j
@Component
@RequiredArgsConstructor
public class UserActionConsumerWorker implements Runnable {
    private final KafkaClientProperties кafkaClientProperties;
    private final AnalyzerService analyzerService;
    private final UserActionKafkaClient kafkaClient;

    private static final Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();

    @Override
    public void run() {
        Runtime.getRuntime().addShutdownHook(new Thread(kafkaClient.getConsumer()::wakeup));
        try {
            while (true) {
                log.info("Getting user-actions...");
                processRecords(kafkaClient.getConsumer().poll(кafkaClientProperties.getConsumer().getPollTimeoutMs()));
            }
        } catch (WakeupException ignored) {

        } catch (Exception e) {
            log.error("Error during process topic {}", кafkaClientProperties.getTopics().getEventSimilarityTopic(), e);
        } finally {
            try {
                kafkaClient.getConsumer().commitSync(currentOffsets);
            } finally {
                log.info("Closing analyzer user-actions consumer...");
                kafkaClient.stop();
            }
        }
    }

    private void processRecords(ConsumerRecords<Void, UserActionAvro> records) {
        for (ConsumerRecord<Void, UserActionAvro> record : records) {
            log.info("Processing record - topic:[{}] partition:[{}] offset:[{}] value: {}",
                    record.topic(), record.partition(), record.offset(), record.value());
            analyzerService.processUserAction(record.value());
            kafkaClient.getConsumer().commitSync();
        }
    }
}
