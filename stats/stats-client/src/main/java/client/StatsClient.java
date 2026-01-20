package client;

import exception.StatsServerUnavailable;
import lombok.RequiredArgsConstructor;
import model.EndpointHitDto;
import model.ViewStatsDto;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cloud.client.ServiceInstance;
import org.springframework.cloud.client.discovery.DiscoveryClient;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;
import org.springframework.web.util.UriComponentsBuilder;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;

@Component
@RequiredArgsConstructor
public class StatsClient {
    private final RestClient.Builder restClientBuilder;
    private final DiscoveryClient discoveryClient;

    @Value("${services.stats-service-id:stats-server}")
    private String statsServiceId;

    private String getStatsServiceUrl() {
        ServiceInstance serviceInstance = discoveryClient.getInstances(statsServiceId).stream()
                .findFirst()
                .orElseThrow(() -> new StatsServerUnavailable("Сервис статистики с id: " + statsServiceId
                        + " не найден в реестре"));

        return String.format("http://%s:%d", serviceInstance.getHost(), serviceInstance.getPort());
    }

    private RestClient getRestClient() {
        return restClientBuilder
                .baseUrl(getStatsServiceUrl())
                .build();
    }

    public void hit(EndpointHitDto endpointHitDto) {
        try {
            RestClient restClient = getRestClient();

            restClient.post()
                    .uri("/hit")
                    .contentType(MediaType.APPLICATION_JSON)
                    .body(endpointHitDto)
                    .retrieve()
                    .toBodilessEntity();
        } catch (StatsServerUnavailable e) {
            throw e;
        } catch (Exception e) {
            System.err.println("Ошибка при отправке статистики: " + e.getMessage());
        }
    }

    public List<ViewStatsDto> getStats(LocalDateTime start, LocalDateTime end,
                                       List<String> uris, Boolean unique) {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

        try {
            UriComponentsBuilder uriBuilder = UriComponentsBuilder.fromPath("/stats")
                    .queryParam("start", start.format(formatter))
                    .queryParam("end", end.format(formatter))
                    .queryParam("unique", unique);

            if (uris != null && !uris.isEmpty()) {
                uris.forEach(uri -> uriBuilder.queryParam("uris", uri));
            }

            RestClient restClient = getRestClient();

            return restClient.get()
                    .uri(uriBuilder.build().toUriString())
                    .retrieve()
                    .body(new ParameterizedTypeReference<>() {});
        } catch (StatsServerUnavailable e) {
            throw e;
        } catch (Exception e) {
            System.err.println("Ошибка при получении статистики: " + e.getMessage());
            return List.of();
        }
    }
}