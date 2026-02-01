package request.service;


import dto.event.EventFullDto;
import dto.request.EventRequestStatusUpdateRequest;
import dto.request.EventRequestStatusUpdateResult;
import dto.request.ParticipationRequestDto;
import feign.event.EventClient;
import feign.user.UserClient;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import request.persistence.model.ParticipationRequest;
import request.persistence.model.RequestStatus;
import request.persistence.mapper.ParticipationRequestMapper;
import request.persistence.repository.ParticipationRequestRepository;
import util.exception.ConflictException;
import util.exception.NotFoundException;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Service
@Slf4j
@RequiredArgsConstructor
public class ParticipationRequestServiceImpl implements ParticipationRequestService {

    private final ParticipationRequestRepository requestRepository;
    private final UserClient userClient;
    private final EventClient eventClient;
    private final ParticipationRequestMapper requestMapper;

    @Override
    public List<ParticipationRequestDto> getUserRequests(Long userId) {
        if (userClient.getById(userId) == null) {
            throw new NotFoundException("Пользователь с id = " + userId + " не найден");
        }

        return requestRepository.findAllByRequesterIdWithEventAndRequester(userId).stream()
                .map(requestMapper::toDto)
                .collect(Collectors.toList());
    }

    @Override
    @Transactional
    public ParticipationRequestDto createRequest(Long userId, Long eventId) {
        log.info("Создание запроса для пользователя с id: {} на событие с id: {}", userId, eventId);

        EventFullDto event = eventClient.getById(eventId);

        if (event == null) {
            throw new NotFoundException("Событие с id=" + eventId + " не найдено");
        }

        if (event.getInitiator() == null) {
            throw new RuntimeException("Не удалось получить автора события");
        }

        if (event.getInitiator().getId().equals(userId)) {
            throw new ConflictException("Нельзя участвовать в собственном событии");
        }

        if (!"PUBLISHED".equals(event.getState())) {
            throw new ConflictException("Нельзя участвовать в неопубликованном событии");
        }

        if (requestRepository.existsByEventAndRequester(eventId, userId)) {
            throw new ConflictException("Запрос на участие в этом событии уже существует");
        }

        Map<Long, Long> confirmedRequestsMap = eventClient.getConfirmedRequestsBatchByEventIds(List.of(eventId));
        Long confirmedRequests = confirmedRequestsMap.getOrDefault(eventId, 0L);

        if (event.getParticipantLimit() > 0 && confirmedRequests >= event.getParticipantLimit()) {
            throw new ConflictException("Достигнут лимит участников для этого события");
        }

        RequestStatus status;
        if (!event.getRequestModeration() || event.getParticipantLimit() == 0) {
            status = RequestStatus.CONFIRMED;
        } else {
            status = RequestStatus.PENDING;
        }

        ParticipationRequest request = ParticipationRequest.builder()
                .requester(userId)
                .event(eventId)
                .created(LocalDateTime.now())
                .status(status)
                .build();

        ParticipationRequest savedRequest = requestRepository.save(request);
        log.info("Запрос создан с id: {}", savedRequest.getId());

        return requestMapper.toDto(savedRequest);
    }

    @Override
    @Transactional
    public EventRequestStatusUpdateResult changeRequestStatus(Long userId, Long eventId,
                                                              EventRequestStatusUpdateRequest request) {
        log.info("Изменение статуса запросов для события с id: {} от пользователя с id: {}", eventId, userId);

        EventFullDto event = eventClient.getById(eventId);

        if (event == null) {
            throw new NotFoundException("Событие с id = " + eventId + " не найдено");
        }

        if (event.getInitiator() == null) {
            throw new RuntimeException("Не удалось получить автора события");
        }

        if (!event.getInitiator().getId().equals(userId)) {
            throw new ConflictException("Пользователь не является инициатором события");
        }

        if (!event.getRequestModeration() || event.getParticipantLimit() == 0) {
            throw new ConflictException("Для этого события не требуется модерация заявок");
        }

        RequestStatus newStatus;
        try {
            newStatus = RequestStatus.valueOf(request.getStatus());
        } catch (IllegalArgumentException e) {
            throw new ConflictException("Недопустимый статус: " + request.getStatus());
        }

        if (newStatus != RequestStatus.CONFIRMED && newStatus != RequestStatus.REJECTED) {
            throw new ConflictException("Можно установить только статусы CONFIRMED или REJECTED");
        }

        List<ParticipationRequest> requestsToUpdate =
                requestRepository.findAllByIdWithEventAndRequester(request.getRequestIds());

        if (requestsToUpdate.size() != request.getRequestIds().size()) {
            throw new NotFoundException("Некоторые запросы не найдены");
        }

        List<ParticipationRequestDto> confirmedRequests = new ArrayList<>();
        List<ParticipationRequestDto> rejectedRequests = new ArrayList<>();

        Map<Long, Long> confirmedRequestsMap = eventClient.getConfirmedRequestsBatchByEventIds(List.of(eventId));
        Long currentConfirmedCount = confirmedRequestsMap.getOrDefault(eventId, 0L);

        int participantLimit = event.getParticipantLimit() != null ? event.getParticipantLimit() : 0;

        for (ParticipationRequest participationRequest : requestsToUpdate) {
            if (participationRequest.getStatus() != RequestStatus.PENDING) {
                throw new ConflictException("Можно изменять только запросы в статусе PENDING. " +
                        "Запрос с id = " + participationRequest.getId() + " имеет статус: " +
                        participationRequest.getStatus());
            }

            if (!participationRequest.getEvent().equals(eventId)) {
                throw new ConflictException("Запрос с id = " + participationRequest.getId() +
                        " не принадлежит событию с id = " + eventId);
            }

            if (newStatus == RequestStatus.CONFIRMED) {
                if (participantLimit > 0 && currentConfirmedCount >= participantLimit) {
                    throw new ConflictException("Достигнут лимит участников для события");
                }
                participationRequest.setStatus(RequestStatus.CONFIRMED);
                currentConfirmedCount++;
                confirmedRequests.add(requestMapper.toDto(participationRequest));
            } else {
                participationRequest.setStatus(RequestStatus.REJECTED);
                rejectedRequests.add(requestMapper.toDto(participationRequest));
            }
        }

        requestRepository.saveAll(requestsToUpdate);

        log.info("Обновлено статусов: подтверждено - {}, отклонено - {}",
                confirmedRequests.size(), rejectedRequests.size());

        return EventRequestStatusUpdateResult.builder()
                .confirmedRequests(confirmedRequests)
                .rejectedRequests(rejectedRequests)
                .build();
    }


    @Override
    @Transactional
    public ParticipationRequestDto cancelRequest(Long userId, Long requestId) {
        ParticipationRequest request = requestRepository.findByIdWithEventAndRequester(requestId, userId)
                .orElseThrow(() -> new NotFoundException("Запрос с id=" + requestId + " не найден"));

        request.setStatus(RequestStatus.CANCELED);
        ParticipationRequest updatedRequest = requestRepository.save(request);

        return requestMapper.toDto(updatedRequest);
    }

    @Override
    public List<ParticipationRequestDto> getRequestsByEvent(Long userId, Long eventId) {
        EventFullDto event = eventClient.getById(eventId);

        if (event == null) {
            throw new NotFoundException("Событие с id = " + eventId + " не найдено");
        }

        if (event.getInitiator() == null) {
            throw new RuntimeException("Не удалось получить автора события");
        }

        if (!event.getInitiator().getId().equals(userId)) {
            throw new ConflictException("Пользователь не является инициатором события");
        }

        return requestRepository.findAllByEventIdWithEventAndRequester(eventId).stream()
                .map(requestMapper::toDto)
                .collect(Collectors.toList());
    }
}
