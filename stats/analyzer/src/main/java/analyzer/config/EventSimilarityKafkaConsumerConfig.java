package analyzer.config;

import analyzer.kafka.EventSimilarityKafkaClient;
import config.KafkaClientProperties;
import kafka.EventSimilarityDeserializer;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;
import ru.practicum.ewm.stats.avro.EventSimilarityAvro;

import java.util.List;
import java.util.Properties;

@Configuration
@RequiredArgsConstructor
public class EventSimilarityKafkaConsumerConfig {
    private final KafkaClientProperties kafkaClientProperties;

    @Bean
    @Scope("prototype")
    EventSimilarityKafkaClient eventSimilarityKafkaClient() {
        return new EventSimilarityKafkaClient() {
            private Consumer<Void, EventSimilarityAvro> eventSimilarityConsumer;

            @Override
            public Consumer<Void, EventSimilarityAvro> getConsumer() {
                if (eventSimilarityConsumer == null) {
                    initEventSimilarityConsumer();
                }
                return eventSimilarityConsumer;
            }

            private void initEventSimilarityConsumer() {
                Properties consumerProps = new Properties();
                consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaClientProperties.getBootstrapServers());
                consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, kafkaClientProperties.getKeyDeserializerClass());
                consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, EventSimilarityDeserializer.class.getName());
                consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, kafkaClientProperties.getConsumer().getGroupIds().getAnalyzerEventSimilarityGroupId());
                consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, kafkaClientProperties.getConsumer().isEnableAutoCommit());

                eventSimilarityConsumer = new KafkaConsumer<>(consumerProps);
                eventSimilarityConsumer.subscribe(List.of(kafkaClientProperties.getTopics().getEventSimilarityTopic()));
            }

            @Override
            public void stop() {
                if (eventSimilarityConsumer != null) {
                    eventSimilarityConsumer.close();
                }
            }
        };
    }
}
