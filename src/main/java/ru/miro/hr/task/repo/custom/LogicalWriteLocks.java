package ru.miro.hr.task.repo.custom;

import lombok.extern.slf4j.Slf4j;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

/**
 * This implementation has liveness problems: operations that require broad range locks can be outpaced by
 * simple transactions with individuals locks. Some fairness mechanics should be added.
 * To guard against deadlocks, lockIndividualZ(..) and lockRand(..) can have timeouts.
 */
@Slf4j
class LogicalWriteLocks {

    // locks associated with z
    private final TreeSet<Integer> individualZLocks = new TreeSet<>();
    // locks associated with id
    private final Set<Integer> individualIdLocks = new HashSet<>();
    // null means no range lock. 10 means all widgets with z >= 10 are locked
    private Integer rangeLock = null;

    synchronized void lockIndividualId(int id) throws InterruptedException {
        while (individualIdLocks.contains(id)) {
            wait();
        }
        individualIdLocks.add(id);
    }

    synchronized void lockIndividualZWithTimeout(long timeout, int... arrayOfZ) throws InterruptedException, TimeoutException {
        long start = System.currentTimeMillis();
        var sortedArrayOfZ = Arrays.stream(arrayOfZ).sorted().distinct().toArray();
        for (int k : sortedArrayOfZ) {
            while ((rangeLock != null && k > rangeLock) || individualZLocks.contains(k)) {
                long timeoutForIteration = timeout - (System.currentTimeMillis() - start);
                if (timeoutForIteration > 0) {
                    wait(timeoutForIteration);
                } else {
                    releaseIndividualZ(Arrays.stream(sortedArrayOfZ).takeWhile(i -> i < k).toArray());
                    throw new TimeoutException();
                }
            }
            individualZLocks.add(k);
        }
    }

    synchronized void lockIndividualZ(int... arrayOfZ) throws InterruptedException {
        try {
            lockIndividualZWithTimeout(Long.MAX_VALUE, arrayOfZ);
        } catch (TimeoutException e) {
            log.warn("Can't be here");
        }
    }

    synchronized void lockRangeWithTimeout(long timeout, int fromZ) throws InterruptedException, TimeoutException {
        long start = System.currentTimeMillis();
        while (rangeLock != null || individualZLocks.higher(fromZ) != null) {
            long timeoutForIteration = timeout - (System.currentTimeMillis() - start);
            if (timeoutForIteration > 0) {
                wait(timeoutForIteration);
            } else {
                throw new TimeoutException();
            }
        }
        rangeLock = fromZ;
    }

    synchronized void lockRange(int fromZ) throws InterruptedException {
        try {
            lockRangeWithTimeout(Long.MAX_VALUE, fromZ);
        } catch (TimeoutException e) {
            log.warn("Can't be here");
        }
    }

    synchronized void releaseIndividualId(int id) {
        individualIdLocks.remove(id);
        notifyAll();
    }

    synchronized void releaseIndividualZ(int... arrayOfZ) {
        individualZLocks.removeAll(Arrays.stream(arrayOfZ).boxed().collect(Collectors.toUnmodifiableSet()));
        notifyAll();
    }

    synchronized void releaseRange() {
        rangeLock = null;
        notifyAll();
    }

    synchronized void reset() {
        rangeLock = null;
        individualZLocks.clear();
        individualIdLocks.clear();
    }
}
