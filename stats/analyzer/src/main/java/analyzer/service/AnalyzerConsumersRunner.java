package analyzer.service;

import lombok.RequiredArgsConstructor;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class AnalyzerConsumersRunner implements CommandLineRunner {
    private final EventSimilarityConsumerWorker eventSimilarityConsumerWorker;
    private final UserActionConsumerWorker userActionConsumerWorker;

    @Override
    public void run(String... args) {
        Thread similaritiesThread = new Thread(eventSimilarityConsumerWorker);
        similaritiesThread.setName("EventSimilarityConsumerThread");
        similaritiesThread.start();

        Thread userActionsThread = new Thread(userActionConsumerWorker);
        userActionsThread.setName("UserActionConsumerThread");
        userActionsThread.start();
    }
}
