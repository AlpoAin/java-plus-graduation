package aggregator.config;

import aggregator.kafka.AggregatorKafkaClient;
import config.KafkaClientProperties;
import kafka.AvroSerializer;
import kafka.UserActionDeserializer;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;
import ru.practicum.ewm.stats.avro.EventSimilarityAvro;
import ru.practicum.ewm.stats.avro.UserActionAvro;

import java.util.List;
import java.util.Properties;

@Configuration
@RequiredArgsConstructor
public class AggregatorKafkaClientConfig {
    private final KafkaClientProperties kafkaClientProperties;

    @Bean
    @Scope("prototype")
    public AggregatorKafkaClient kafkaClient() {
        return new AggregatorKafkaClient() {
            private Consumer<Void, UserActionAvro> userActionConsumer;
            private Producer<Void, EventSimilarityAvro> similarityProducer;

            @Override
            public Producer<Void, EventSimilarityAvro> getProducer() {
                if (similarityProducer == null) {
                    initProducer();
                }
                return similarityProducer;
            }

            @Override
            public Consumer<Void, UserActionAvro> getConsumer() {
                if (userActionConsumer == null) {
                    initConsumer();
                }
                return userActionConsumer;
            }

            private void initProducer() {
                Properties producerProps = new Properties();
                producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaClientProperties.getBootstrapServers());
                producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, kafkaClientProperties.getKeySerializerClass());
                producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, AvroSerializer.class.getName());
                producerProps.put(ProducerConfig.LINGER_MS_CONFIG, kafkaClientProperties.getProducer().getLingerMs());

                similarityProducer = new KafkaProducer<>(producerProps);
            }

            private void initConsumer() {
                Properties consumerProps = new Properties();
                consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaClientProperties.getBootstrapServers());
                consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, kafkaClientProperties.getKeyDeserializerClass());
                consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, UserActionDeserializer.class.getName());
                consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, kafkaClientProperties.getConsumer().getGroupIds().getAggregatorUserActionsGroupId());
                consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, kafkaClientProperties.getConsumer().isEnableAutoCommit());

                userActionConsumer = new KafkaConsumer<>(consumerProps);
                userActionConsumer.subscribe(List.of(kafkaClientProperties.getTopics().getUserActionsTopic()));
            }

            @Override
            public void stop() {
                if (similarityProducer != null) {
                    similarityProducer.close();
                }
                if (userActionConsumer != null) {
                    userActionConsumer.close();
                }
            }
        };
    }
}
