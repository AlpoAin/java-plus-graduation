package analyzer.config;

import analyzer.kafka.UserActionKafkaClient;
import config.KafkaClientProperties;
import kafka.UserActionDeserializer;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;
import ru.practicum.ewm.stats.avro.UserActionAvro;

import java.util.List;
import java.util.Properties;

@Configuration
@RequiredArgsConstructor
public class UserActionKafkaConsumerConfig {
    private final KafkaClientProperties kafkaClientProperties;

    @Bean
    @Scope("prototype")
    UserActionKafkaClient userActionKafkaClient() {
        return new UserActionKafkaClient() {
            private Consumer<Void, UserActionAvro> userActionConsumer;

            @Override
            public Consumer<Void, UserActionAvro> getConsumer() {
                if (userActionConsumer == null) {
                    initUserActionConsumer();
                }
                return userActionConsumer;
            }

            private void initUserActionConsumer() {
                Properties consumerProps = new Properties();
                consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaClientProperties.getBootstrapServers());
                consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, kafkaClientProperties.getKeyDeserializerClass());
                consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, UserActionDeserializer.class.getName());
                consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, kafkaClientProperties.getConsumer().getGroupIds().getAnalyzerUserActionsGroupId());
                consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, kafkaClientProperties.getConsumer().isEnableAutoCommit());

                userActionConsumer = new KafkaConsumer<>(consumerProps);
                userActionConsumer.subscribe(List.of(kafkaClientProperties.getTopics().getUserActionsTopic()));
            }

            @Override
            public void stop() {
                if (userActionConsumer != null) {
                    userActionConsumer.close();
                }
            }
        };
    }
}
