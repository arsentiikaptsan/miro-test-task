package ru.miro.hr.task.repo.custom;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.*;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.fail;

public class LogicalWriteLocksUT {

    private final static ExecutorService executorService = Executors.newSingleThreadExecutor();
    private final LogicalWriteLocks logicalWriteLocks = new LogicalWriteLocks();

    @AfterAll
    public static void shutDownExecutor() {
        executorService.shutdown();
    }

    @AfterEach
    public void reset() {
        logicalWriteLocks.reset();
    }

    @Test
    @Timeout(1)
    public void testSimpleLockAndRelease() throws InterruptedException {
        logicalWriteLocks.lockIndividualId(1);
        logicalWriteLocks.lockIndividualZ(1, 2);
        logicalWriteLocks.lockRange(2);

        logicalWriteLocks.releaseRange();
        logicalWriteLocks.releaseIndividualZ(1, 2);
        logicalWriteLocks.releaseIndividualId(1);
    }

    @Test
    public void testBlockingOnConflict() throws InterruptedException, TimeoutException, ExecutionException {
        logicalWriteLocks.lockRange(2);

        Future future = executorService.submit(() -> {
            try {
                logicalWriteLocks.lockIndividualZ(3, 1);
            } catch (InterruptedException ignored) {
            }
        });

        Thread.sleep(100);

        assertThat(future.isDone()).isFalse();

        logicalWriteLocks.releaseRange();
        try {
            future.get(1, TimeUnit.SECONDS);
            assertThat(future.isDone()).isTrue();
        } catch (TimeoutException e) {
            future.cancel(true);
            fail("Waiting for too long");
        }
    }

    @Test
    public void testTimeouts() throws InterruptedException, ExecutionException {
        logicalWriteLocks.lockIndividualZ(1);

        Future<Boolean> future = executorService.submit(() -> {
            try {
                logicalWriteLocks.lockIndividualZWithTimeout(100L, 1);
            } catch (TimeoutException e) {
                return true;
            } catch (InterruptedException ignored) {
            }
            return false;
        });
        try {
            boolean wasTimeout = future.get(1, TimeUnit.SECONDS);
            assertThat(wasTimeout).isTrue();
        } catch (TimeoutException e) {
            future.cancel(true);
            fail("Waiting for too long");
        }
    }
}
