package collector.config;

import collector.kafka.CollectorProducerClient;
import config.KafkaClientProperties;
import kafka.AvroSerializer;
import lombok.RequiredArgsConstructor;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;

import java.util.Properties;

@Configuration
@RequiredArgsConstructor
public class CollectorKafkaProducerConfig {
    private final KafkaClientProperties kafkaClientProperties;

    @Bean
    @Scope("prototype")
    CollectorProducerClient collectorKafkaClient() {
        return new CollectorProducerClient() {
            private Producer<Void, SpecificRecordBase> userActionProducer;

            @Override
            public Producer<Void, SpecificRecordBase> getProducer() {
                if (userActionProducer == null) {
                    initUserActionProducer();
                }
                return userActionProducer;
            }

            private void initUserActionProducer() {
                Properties producerProps = new Properties();
                producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaClientProperties.getBootstrapServers());
                producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, kafkaClientProperties.getKeySerializerClass());
                producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, AvroSerializer.class.getName());
                producerProps.put(ProducerConfig.LINGER_MS_CONFIG, kafkaClientProperties.getProducer().getLingerMs());

                userActionProducer = new KafkaProducer<>(producerProps);
            }

            @Override
            public void stop() {
                if (userActionProducer != null) {
                    userActionProducer.close();
                }
            }
        };
    }
}
