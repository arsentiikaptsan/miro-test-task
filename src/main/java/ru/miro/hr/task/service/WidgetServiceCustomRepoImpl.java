package ru.miro.hr.task.service;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.retry.Retry;
import ru.miro.hr.task.exception.ResourceNotFoundException;
import ru.miro.hr.task.exception.TimeoutRuntimeException;
import ru.miro.hr.task.model.Widget;
import ru.miro.hr.task.repo.custom.CustomRepo;
import ru.miro.hr.task.rest.dto.WidgetDto;

import java.time.Duration;
import java.util.Optional;

@Service("widgetServiceCustomRepo")
@ConditionalOnProperty(name = "spring.profiles.active", havingValue = "custom_repo")
public class WidgetServiceCustomRepoImpl implements WidgetService {

    private final CustomRepo repo;
    private final Retry retryPolicy;

    public WidgetServiceCustomRepoImpl(CustomRepo repo,
                                       @Value("${transaction.retry-number:3}") int retryNumber,
                                       @Value("${transaction.retry-min-backoff:10}") int minBackoff,
                                       @Value("${transaction.retry-max-backoff:1000}") int maxBackoff) {
        this.repo = repo;
        this.retryPolicy = Retry
                .anyOf(TimeoutRuntimeException.class)
                .retryMax(retryNumber)
                .exponentialBackoff(Duration.ofMillis(minBackoff), Duration.ofMillis(maxBackoff));
    }

    @Override
    public Mono<Widget> getWidget(int id) {
        return Mono.fromCallable(() -> Optional.ofNullable(repo.getWidgetById(id))
                .orElseThrow(ResourceNotFoundException::new));
    }

    @Override
    public Flux<Widget> getWidgets(int from, int size) {
        return repo.getWidgetsOrderByZAscAndStartingAtAndLimitBy(from, size);
    }

    @Override
    public Mono<Integer> getWidgetsCount() {
        return Mono.just(repo.size());
    }

    @Override
    public Mono<Widget> createWidget(WidgetDto dto) {
        Mono<Widget> result;
        if (dto.getZ() != null) {
            result = Mono.fromCallable(() -> repo.createWidgetById(
                    dto.getX(),
                    dto.getY(),
                    dto.getZ(),
                    dto.getHeight(),
                    dto.getWidth()));
        } else {
            result = Mono.fromCallable(() -> repo.createWidgetByIdWithMaxZ(
                    dto.getX(),
                    dto.getY(),
                    dto.getHeight(),
                    dto.getWidth()));
        }
        return result.subscribeOn(Schedulers.boundedElastic());
    }

    @Override
    public Mono<Widget> updateWidget(WidgetDto dto) {
        Mono<Widget> result;
        if (dto.getZ() != null) {
            result = Mono.fromCallable(() -> repo.updateWidgetById(
                    dto.getId(),
                    dto.getX(),
                    dto.getY(),
                    dto.getZ(),
                    dto.getHeight(),
                    dto.getWidth()));
        } else {
            result = Mono.fromCallable(() -> repo.updateWidgetByIdToMaxZ(
                    dto.getId(),
                    dto.getX(),
                    dto.getY(),
                    dto.getHeight(),
                    dto.getWidth()));
        }
        return result
                .retryWhen(reactor.util.retry.Retry.withThrowable(retryPolicy))
                .subscribeOn(Schedulers.boundedElastic());
    }

    @Override
    public Mono<Void> deleteWidget(int id) {
        return Mono.<Void>fromRunnable(() -> repo.deleteWidgetById(id))
                .retryWhen(reactor.util.retry.Retry.withThrowable(retryPolicy))
                .subscribeOn(Schedulers.boundedElastic());
    }
}
