package ru.miro.hr.task.service;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import ru.miro.hr.task.exception.ResourceNotFoundException;
import ru.miro.hr.task.model.Widget;
import ru.miro.hr.task.repo.custom.CustomRepo;
import ru.miro.hr.task.rest.dto.WidgetDto;

@Service("widgetServiceCustomRepo")
@ConditionalOnProperty(name = "spring.profiles.active", havingValue = "custom_repo")
public class WidgetServiceCustomRepoImpl implements WidgetService {

    private final CustomRepo repo;
    private final int retryNumber;

    public WidgetServiceCustomRepoImpl(CustomRepo repo,
                                       @Value("${custom-service.retry-number:3}") int retryNumber) {
        this.repo = repo;
        this.retryNumber = retryNumber;
    }

    @Override
    public Mono<Widget> getWidget(int id) {
        var result = repo.getWidgetById(id);
        if (result == null) {
            return Mono.error(ResourceNotFoundException::new);
        }
        return Mono.just(result);
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
                .retry(retryNumber)
                .subscribeOn(Schedulers.boundedElastic());
    }

    @Override
    public Mono deleteWidget(int id) {
        return Mono.fromRunnable(() -> repo.deleteWidgetById(id))
                .retry(retryNumber)
                .subscribeOn(Schedulers.boundedElastic());
    }
}
