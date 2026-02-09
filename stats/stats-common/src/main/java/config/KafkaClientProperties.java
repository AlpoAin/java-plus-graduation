package config;

import jakarta.annotation.PostConstruct;
import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Component
@Getter
@Setter
@ConfigurationProperties(prefix = "spring.kafka.properties")
public class KafkaClientProperties {

    private String bootstrapServers;
    private String keySerializerClass;
    private String keyDeserializerClass;

    private Consumer consumer = new Consumer();
    private Producer producer = new Producer();
    private Topics topics = new Topics();

    @Getter
    @Setter
    public static class Consumer {
        private boolean enableAutoCommit;
        private int pollTimeoutMs;
        private GroupIds groupIds = new GroupIds();

        @Getter
        @Setter
        public static class GroupIds {
            private String aggregatorUserActionsGroupId;
            private String analyzerUserActionsGroupId;
            private String analyzerEventSimilarityGroupId;
        }
    }

    @Getter
    @Setter
    public static class Producer {
        private int lingerMs;
    }

    @Getter
    @Setter
    public static class Topics {
        private String userActionsTopic;
        private String eventSimilarityTopic;
    }
}
