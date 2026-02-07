package aggregator;

import aggregator.service.EventSimilarityAggregator;
import config.KafkaClientProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ConfigurableApplicationContext;

@SpringBootApplication
@EnableConfigurationProperties(KafkaClientProperties.class)
public class AggregatorApplication {
    public static void main(String[] args) {
        ConfigurableApplicationContext context = SpringApplication.run(AggregatorApplication.class, args);

        EventSimilarityAggregator aggregator = context.getBean(EventSimilarityAggregator.class);
        aggregator.start();
    }
}
