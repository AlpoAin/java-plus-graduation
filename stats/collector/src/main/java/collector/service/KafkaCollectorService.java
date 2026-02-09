package collector.service;

import collector.kafka.CollectorProducerClient;
import collector.mapper.UserActionAvroMapper;
import config.KafkaClientProperties;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.grpc.stats.user.UserActionProto;

@Slf4j
@Service
@RequiredArgsConstructor
public class KafkaCollectorService implements CollectorService {
    private final KafkaClientProperties kafkaProperties;
    private final CollectorProducerClient producerClient;

    @Override
    public void publishUserAction(UserActionProto userActionProto) {
        producerClient.getProducer().send(
                new ProducerRecord<>(
                        kafkaProperties.getTopics().getUserActionsTopic(),
                        null,
                        UserActionAvroMapper.toAvro(userActionProto)
                )
        );

        log.info("Sent user action to Kafka: {}", userActionProto);
    }
}
