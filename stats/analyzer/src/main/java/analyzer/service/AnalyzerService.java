package analyzer.service;

import ru.practicum.ewm.stats.avro.EventSimilarityAvro;
import ru.practicum.ewm.stats.avro.UserActionAvro;

import java.util.List;
import java.util.Map;

public interface AnalyzerService {
    Map<Long, Double> getInteractionsCount(List<Long> eventIds);

    Map<Long, Double> getSimilarEvents(Long eventId, Long userId, Integer limit);

    Map<Long, Double> getRecommendations(Long userId, Integer limit);

    void processEventSimilarity(EventSimilarityAvro eventSimilarityAvro);

    void processUserAction(UserActionAvro userActionAvro);
}
