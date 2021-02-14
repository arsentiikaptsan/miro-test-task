package ru.miro.hr.task.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.lang.NonNull;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import ru.miro.hr.task.exception.ResourceNotFoundException;
import ru.miro.hr.task.model.Widget;
import ru.miro.hr.task.repo.jpa.JpaRepo;
import ru.miro.hr.task.repo.jpa.WidgetJpaImpl;
import ru.miro.hr.task.rest.dto.WidgetDto;

@Service("widgetServiceJpa")
@ConditionalOnProperty(name = "spring.profiles.active", havingValue = "jpa_repo")
@Slf4j
public class WidgetServiceJpaImpl implements WidgetService {

    private final JpaRepo repo;
    private final TransactionTemplate transactionTemplate;

    @Autowired
    public WidgetServiceJpaImpl(JpaRepo repo, PlatformTransactionManager transactionManager) {
        this.repo = repo;
        this.transactionTemplate = new TransactionTemplate(transactionManager);
    }

    @Override
    public Mono<Widget> getWidget(int id) {
        return Mono.fromCallable(() -> repo.findById(id).orElseThrow(ResourceNotFoundException::new))
                .map(w -> (Widget) w)
                .subscribeOn(Schedulers.boundedElastic());
    }

    @Override
    public Flux<Widget> getWidgets(int from, int size) {
        return Flux.fromStream(repo.findPageOrderedByZ(from, size).stream())
                .map(w -> (Widget) w)
                .subscribeOn(Schedulers.boundedElastic());
    }

    @Override
    public Mono<Integer> getWidgetsCount() {
        return Mono.fromCallable(() -> (int) repo.count());
    }

    @Override
    public Mono<Widget> createWidget(@NonNull WidgetDto dto) {
        return Mono.fromCallable(() -> transactionTemplate.execute(status -> {
            var newWidget = new WidgetJpaImpl(dto.getX(), dto.getY(), 0, dto.getWidth(), dto.getHeight());
            if (dto.getZ() == null) {
                int newZ = repo.findMaxZ().map(z -> z + 1).orElse(0);
                newWidget.setZ(newZ);
            } else {
                if (repo.existsByZ(dto.getZ())) {
                    repo.moveUp(dto.getZ());
                }
                newWidget.setZ(dto.getZ());
            }

            return repo.save(newWidget);
        }))
                .map(w -> (Widget) w)
                .subscribeOn(Schedulers.boundedElastic());
    }

    @Override
    public Mono<Widget> updateWidget(@NonNull WidgetDto dto) {
        return Mono.fromCallable(() -> transactionTemplate.execute(status -> {
            var widget = repo.findById(dto.getId()).orElseThrow(ResourceNotFoundException::new);
            widget.setX(dto.getX());
            widget.setY(dto.getY());
            widget.setWidth(dto.getWidth());
            widget.setHeight(dto.getHeight());

            if (dto.getZ() == null) {
                int newZ = repo.findMaxZ().map(z -> widget.getZ() == z ? z : z + 1).orElse(0);
                widget.setZ(newZ);
            } else {
                if (repo.existsByZ(dto.getZ())) {
                    repo.moveUp(dto.getZ());
                }
                widget.setZ(dto.getZ());
            }

            return repo.save(widget);
        }))
                .map(w -> (Widget) w)
                .subscribeOn(Schedulers.boundedElastic());
    }

    @Override
    public Mono deleteWidget(int id) {
        return Mono.fromRunnable(() -> {
            int modifiedRows = repo.deleteInOneQuery(id);
            if (modifiedRows == 0) {
                throw new ResourceNotFoundException();
            }
        })
                .subscribeOn(Schedulers.boundedElastic());
    }
}
