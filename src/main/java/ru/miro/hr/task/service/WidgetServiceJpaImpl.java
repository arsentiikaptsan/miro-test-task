package ru.miro.hr.task.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.dao.CannotAcquireLockException;
import org.springframework.dao.QueryTimeoutException;
import org.springframework.lang.NonNull;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionTimedOutException;
import org.springframework.transaction.support.TransactionTemplate;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.retry.Retry;
import ru.miro.hr.task.exception.ResourceNotFoundException;
import ru.miro.hr.task.model.Widget;
import ru.miro.hr.task.repo.jpa.JpaRepo;
import ru.miro.hr.task.repo.jpa.WidgetJpaImpl;
import ru.miro.hr.task.rest.dto.WidgetDto;

import javax.persistence.LockTimeoutException;
import javax.persistence.PessimisticLockException;
import java.time.Duration;

@Service("widgetServiceJpa")
@ConditionalOnProperty(name = "spring.profiles.active", havingValue = "jpa_repo")
@Slf4j
public class WidgetServiceJpaImpl implements WidgetService {

    private final JpaRepo repo;
    private final TransactionTemplate transactionTemplate;
    private final TransactionTemplate readonlyTransactionTemplate;
    private final Retry retryPolicy;

    @Autowired
    public WidgetServiceJpaImpl(JpaRepo repo,
                                PlatformTransactionManager transactionManager,
                                @Value("${transaction.timeout:5000}") int timeoutInMilliseconds,
                                @Value("${transaction.retry-number:1}") int retryNumber,
                                @Value("${transaction.retry-min-backoff:10}") int minBackoff,
                                @Value("${transaction.retry-max-backoff:1000}") int maxBackoff) {
        if (timeoutInMilliseconds <= 0) {
            throw new IllegalArgumentException("Timeout must be positive");
        }
        this.repo = repo;
        this.retryPolicy = Retry
                .anyOf(TransactionTimedOutException.class,
                        QueryTimeoutException.class,
                        LockTimeoutException.class,
                        PessimisticLockException.class,
                        CannotAcquireLockException.class)
                .retryMax(retryNumber)
                .exponentialBackoff(Duration.ofMillis(minBackoff), Duration.ofMillis(maxBackoff));
        this.transactionTemplate = new TransactionTemplate(transactionManager);
        this.transactionTemplate.setIsolationLevel(TransactionDefinition.ISOLATION_SERIALIZABLE);
        this.transactionTemplate.setTimeout(Math.max(10, timeoutInMilliseconds / 1000));
        this.readonlyTransactionTemplate = new TransactionTemplate(transactionManager);
        this.readonlyTransactionTemplate.setIsolationLevel(TransactionDefinition.ISOLATION_SERIALIZABLE);
        this.readonlyTransactionTemplate.setReadOnly(true);
        this.readonlyTransactionTemplate.setTimeout(Math.max(10, timeoutInMilliseconds / 1000));
    }

    @Override
    public Mono<? extends Widget> getWidget(int id) {
        return Mono.fromCallable(() ->
                readonlyTransactionTemplate.execute(status ->
                        repo.findById(id).orElseThrow(ResourceNotFoundException::new)))
                .retryWhen(reactor.util.retry.Retry.withThrowable(retryPolicy))
                .subscribeOn(Schedulers.boundedElastic());
    }

    @Override
    public Flux<? extends Widget> getWidgets(int from, int size) {
        return Flux.fromStream(
                readonlyTransactionTemplate.execute(status -> repo.findPageOrderedByZ(from, size))
                        .stream())
                .retryWhen(reactor.util.retry.Retry.withThrowable(retryPolicy))
                .subscribeOn(Schedulers.boundedElastic());
    }

    @Override
    public Mono<Integer> getWidgetsCount() {
        return Mono.fromCallable(() ->
                readonlyTransactionTemplate.execute(status -> (int) repo.count()))
                .retryWhen(reactor.util.retry.Retry.withThrowable(retryPolicy))
                .subscribeOn(Schedulers.boundedElastic());
    }

    @Override
    public Mono<? extends Widget> createWidget(@NonNull WidgetDto dto) {
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
                .retryWhen(reactor.util.retry.Retry.withThrowable(retryPolicy))
                .subscribeOn(Schedulers.boundedElastic());
    }

    @Override
    public Mono<? extends Widget> updateWidget(@NonNull WidgetDto dto) {
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
                .retryWhen(reactor.util.retry.Retry.withThrowable(retryPolicy))
                .subscribeOn(Schedulers.boundedElastic());
    }

    @Override
    public Mono<Void> deleteWidget(int id) {
        return Mono.<Void>fromRunnable(() -> transactionTemplate.executeWithoutResult(status -> {
            int modifiedRows = repo.deleteInOneQuery(id);
            if (modifiedRows == 0) {
                throw new ResourceNotFoundException("Widget.id=" + id);
            }
        }))
                .retryWhen(reactor.util.retry.Retry.withThrowable(retryPolicy))
                .subscribeOn(Schedulers.boundedElastic());
    }
}
