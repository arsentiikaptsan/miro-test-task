package ru.miro.hr.task.repo.custom;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Lazy;
import org.springframework.lang.NonNull;
import org.springframework.lang.Nullable;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import ru.miro.hr.task.exception.ResourceNotFoundException;
import ru.miro.hr.task.exception.TimeoutRuntimeException;
import ru.miro.hr.task.model.Widget;

import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

/**
 * Ok, we are building our own simplified MVCC engine. The goal is to make writes never block reads.
 * Since 90% of load comes from reads, that would significantly improve a throughput.
 * We also try to make writes block each other as little as possible.
 * Here is how we do it: every widget is represented by the set of it's versions throughout time.
 * Updates produce a copy (except for updated fields) of a target widget and old version is kept intact (almost).
 * Each "version object" contains two fields: from and till what "point" it was active (represented current state of a widget).
 * "Point" is a serial number of a transaction in the transaction log. Transaction is considered committed, when it's recorded in log.
 * Each read transaction gets a serial number - the latest committed transaction and then returns only those records,
 * for whom from <= serialNumber < till.
 * Periodically we run vacuum task, that cleans up old records (a.k.a. "version objects" a.k.a. WidgetCustomImpl)
 * which are not visible to any running transaction.
 * Writes though do block each other, because otherwise we would not be able to enforce serialization. There are
 * individual locks and range locks. Thanks to problem's specifics, there can be only one active range lock at any time:
 * for example on all z > 10 (those are acquired when widget's new z is not unique and every widget above this one
 * must be moved up). If z is update from 1 to 2 for example, than locks must be acquired for both 1 and 2. It is done
 * in ascending order to avoid deadlocks. Deadlocks are still possible though: some operations first acquire
 * lock on id and then on z, and some in the reverse order. To guard against deadlocks, all (almost)
 * operations that work with locks by the first pattern would backoff and retry later.
 */
@Component
@Lazy
@Slf4j
public class CustomRepo {

    private final ReentrantReadWriteLock globalLock = new ReentrantReadWriteLock();

    private final ConcurrentHashMap<Integer, ConcurrentLinkedDeque<WidgetCustomImpl>> widgetById;

    // Java SE does not have a concurrent multimap (bag), that's why I have to make z-index key unique.
    // Here comes UniqueZKey thing.
    private final ConcurrentSkipListMap<UniqueKey, WidgetCustomImpl> widgetByZ;
    private final UniqueKeyFactory uniqueKeyFactory = new UniqueKeyFactory();

    private final TransactionLog transactionLog;
    private final LogicalWriteLocks logicalWriteLocks = new LogicalWriteLocks();

    private final AtomicInteger transactionIdSequence = new AtomicInteger(0);
    private final AtomicInteger widgetIdSequence = new AtomicInteger(0);

    private final ConcurrentSkipListSet<Integer> runningReadsSerials = new ConcurrentSkipListSet<>();

    private final long timeout;

    public CustomRepo(@Value("${custom-repo.initial-capacity:1000}") int initialCapacity,
                      @Value("${transaction.timeout:1000}") long timeout) {
        if (timeout <= 0) {
            throw new IllegalArgumentException("Timeout must be positive");
        }
        this.widgetById = new ConcurrentHashMap<>(initialCapacity);
        this.widgetByZ = new ConcurrentSkipListMap<>();
        this.transactionLog = new TransactionLog(initialCapacity);
        this.timeout = timeout;
    }

    @Nullable
    public Widget getWidgetById(int id) {
        return executeInGlobalLockSection(() -> {
            // we need records that are created before or equal to serial and "deleted" after serial
            int serial = transactionLog.getLatestSerial();
            runningReadsSerials.add(serial);

            WidgetCustomImpl result = null;

            var iter = Optional.ofNullable(widgetById.get(id))
                    .map(ConcurrentLinkedDeque::descendingIterator).orElse(null);
            if (iter != null) {
                while (iter.hasNext()) {
                    var record = iter.next();

                    // checking widget is deleted
                    var recordStatus = recordStatus(record, serial);
                    if (recordStatus == RecordStatus.ACTIVE) {
                        result = record;
                        break;
                    } else if (recordStatus == RecordStatus.EXPIRED) {
                        break;
                    }
                }
            }
            runningReadsSerials.remove(serial);
            return result;
        });
    }

    public Flux<Widget> getWidgetsOrderByZAscAndStartingAtAndLimitBy(int from, int size) {
        globalLock.readLock().lock();

        // we need records that are created before or equal to serial and "deleted" after serial
        int serial = transactionLog.getLatestSerial();
        runningReadsSerials.add(serial);

        return Flux.fromStream(widgetByZ.tailMap(new UniqueKey(from, Integer.MIN_VALUE)).values().stream()
                .filter(record -> recordStatus(record, serial) == RecordStatus.ACTIVE)
                .limit(size))
                .cast(Widget.class)
                .doFinally(signalType -> {
                    runningReadsSerials.remove(serial);
                    globalLock.readLock().unlock();
                });
    }

    @SuppressWarnings("ConstantConditions")
    public int size() {
        return executeInGlobalLockSection(() -> {
            int serial = transactionLog.getLatestSerial();
            runningReadsSerials.add(serial);

            int result = (int) widgetById.values().stream().filter(dequeOfRecords -> {
                var iter = dequeOfRecords.descendingIterator();
                while (iter.hasNext()) {
                    var record = iter.next();

                    // checking widget is deleted
                    var recordStatus = recordStatus(record, serial);
                    if (recordStatus == RecordStatus.ACTIVE) {
                        return true;
                    } else if (recordStatus == RecordStatus.EXPIRED) {
                        return false;
                    }
                }
                return false;
            }).count();

            runningReadsSerials.remove(serial);
            return result;
        });
    }

    public void deleteWidgetById(int id) {
        executeInGlobalLockSection(() -> {
            int transactionId = transactionIdSequence.getAndIncrement();

            logicalWriteLocks.lockIndividualId(id);

            try {
                var record = Optional.ofNullable(widgetById.get(id))
                        .flatMap(deque -> deque.isEmpty() ? Optional.empty() : Optional.of(deque.getLast()))
                        .filter(r -> recordStatus(r, Integer.MAX_VALUE) == RecordStatus.ACTIVE)
                        .orElseThrow(ResourceNotFoundException::new);

                logicalWriteLocks.lockIndividualZWithTimeout(timeout, record.getZ());
                record.setTillTransactionId(transactionId);
                int serial = transactionLog.commit(transactionId);
                logicalWriteLocks.releaseIndividualZ(record.getZ());
                record.setTillTransactionSerial(serial);
            } finally {
                logicalWriteLocks.releaseIndividualId(id);
            }

            return null;
        });
    }

    public Widget createWidgetById(int x, int y, int z, int height, int width) {
        return executeInGlobalLockSection(() -> {
            int transactionId = transactionIdSequence.getAndIncrement();
            int newId = widgetIdSequence.getAndIncrement();

            logicalWriteLocks.lockIndividualId(newId);
            logicalWriteLocks.lockIndividualZ(z);

            var newRecord = new WidgetCustomImpl(newId, x, y, z, width, height, transactionId);
            ConcurrentLinkedDeque<WidgetCustomImpl> dequeToInsert = new ConcurrentLinkedDeque<>();
            dequeToInsert.addLast(newRecord);
            widgetById.put(newId, dequeToInsert);
            widgetByZ.put(uniqueKeyFactory.makeUnique(z), newRecord);
            // finds only ACTIVE records, so newly added will not be returned
            var existingRecordWithZ = findRecordByZ(z, Integer.MAX_VALUE);
            List<WidgetCustomImpl> newRecords = null;
            if (existingRecordWithZ != null) {
                logicalWriteLocks.lockRange(z);

                newRecords = widgetByZ
                        .subMap(new UniqueKey(z, Integer.MIN_VALUE), new UniqueKey(Integer.MAX_VALUE, Integer.MAX_VALUE))
                        .values().stream()
                        .filter(record -> recordStatus(record, Integer.MAX_VALUE) == RecordStatus.ACTIVE)
                        .map(oldVersion -> {
                            try {
                                logicalWriteLocks.lockIndividualId(oldVersion.getId());
                            } catch (InterruptedException e) {
                                throw new RuntimeException("Was interrupted");
                            }
                            oldVersion.setTillTransactionId(transactionId);
                            var newVersion = new WidgetCustomImpl(
                                    oldVersion.getId(),
                                    oldVersion.getX(),
                                    oldVersion.getY(),
                                    oldVersion.getZ() + 1,
                                    oldVersion.getWidth(),
                                    oldVersion.getHeight(),
                                    transactionId);
                            widgetById.get(newVersion.getId()).addLast(newVersion);
                            return newVersion;
                        })
                        .collect(Collectors.toUnmodifiableList());

                widgetByZ.putAll(newRecords.stream()
                        .collect(Collectors.toUnmodifiableMap(r -> uniqueKeyFactory.makeUnique(r.getZ()), r -> r)));
            }

            int serial = transactionLog.commit(transactionId);

            logicalWriteLocks.releaseIndividualZ(z);
            logicalWriteLocks.releaseIndividualId(newId);
            if (existingRecordWithZ != null) {
                logicalWriteLocks.releaseRange();
            }
            if (newRecords != null) {
                newRecords.forEach(record -> logicalWriteLocks.releaseIndividualId(record.getId()));
            }
            newRecord.setFromTransactionSerial(serial);

            return newRecord;
        });
    }

    public Widget createWidgetByIdWithMaxZ(int x, int y, int height, int width) {
        return executeInGlobalLockSection(() -> {
            int transactionId = transactionIdSequence.getAndIncrement();
            int newId = widgetIdSequence.getAndIncrement();

            logicalWriteLocks.lockIndividualId(newId);
            logicalWriteLocks.lockIndividualZ(Integer.MIN_VALUE);
            logicalWriteLocks.lockRange(Integer.MIN_VALUE);

            // if this is first widget - set z = 0
            int zForNewRecord = widgetByZ.descendingMap().values().stream()
                    .filter(record -> recordStatus(record, Integer.MAX_VALUE) == RecordStatus.ACTIVE)
                    .findFirst().map(r -> r.getZ() + 1).orElse(0);

            var newRecord = new WidgetCustomImpl(newId, x, y, zForNewRecord, width, height, transactionId);
            ConcurrentLinkedDeque<WidgetCustomImpl> dequeToInsert = new ConcurrentLinkedDeque<>();
            dequeToInsert.addLast(newRecord);
            widgetById.put(newId, dequeToInsert);
            widgetByZ.put(uniqueKeyFactory.makeUnique(zForNewRecord), newRecord);

            int serial = transactionLog.commit(transactionId);

            logicalWriteLocks.releaseRange();
            logicalWriteLocks.releaseIndividualZ(Integer.MIN_VALUE);
            logicalWriteLocks.releaseIndividualId(newId);
            newRecord.setFromTransactionSerial(serial);

            return newRecord;
        });
    }

    public Widget updateWidgetById(int id, int x, int y, int z, int height, int width) {
        return executeInGlobalLockSection(() -> {
            int transactionId = transactionIdSequence.getAndIncrement();

            logicalWriteLocks.lockIndividualId(id);
            var oldRecord = Optional.ofNullable(widgetById.get(id))
                    .flatMap(deque -> deque.isEmpty() ? Optional.empty() : Optional.of(deque.getLast()))
                    .filter(r -> recordStatus(r, Integer.MAX_VALUE) == RecordStatus.ACTIVE)
                    .orElseThrow(() -> {
                        logicalWriteLocks.releaseIndividualId(id);
                        return new ResourceNotFoundException("Widget.id=" + id);
                    });

            var newRecord = new WidgetCustomImpl(id, x, y, z, width, height, transactionId);
            // Check ig any field is different
            if (!oldRecord.equals(newRecord)) {
                List<WidgetCustomImpl> newRecords = null;
                boolean isRangeLockAcquired = false;

                try {
                    logicalWriteLocks.lockIndividualZWithTimeout(timeout, z, oldRecord.getZ());
                } catch (TimeoutException e) {
                    logicalWriteLocks.releaseIndividualId(id);
                    throw e;
                }

                if (z != oldRecord.getZ() && findRecordByZ(z, Integer.MAX_VALUE) != null) {
                    isRangeLockAcquired = true;

                    try {
                        logicalWriteLocks.lockRangeWithTimeout(timeout, z);
                    } catch (TimeoutException e) {
                        logicalWriteLocks.releaseIndividualId(id);
                        logicalWriteLocks.releaseIndividualZ(z, oldRecord.getZ());
                        throw e;
                    }

                    newRecords = widgetByZ
                            .subMap(new UniqueKey(z, Integer.MIN_VALUE), new UniqueKey(Integer.MAX_VALUE, Integer.MAX_VALUE))
                            .values().stream()
                            .filter(record -> recordStatus(record, Integer.MAX_VALUE) == RecordStatus.ACTIVE)
                            .map(oldVersion -> {
                                try {
                                    logicalWriteLocks.lockIndividualId(oldVersion.getId());
                                } catch (InterruptedException e) {
                                    throw new RuntimeException("Got interrupted, exiting");
                                }
                                oldVersion.setTillTransactionId(transactionId);
                                var newVersion = new WidgetCustomImpl(
                                        oldVersion.getId(),
                                        oldVersion.getX(),
                                        oldVersion.getY(),
                                        oldVersion.getZ() + 1,
                                        oldVersion.getWidth(),
                                        oldVersion.getHeight(),
                                        transactionId);
                                widgetById.get(newVersion.getId()).addLast(newVersion);
                                return newVersion;
                            })
                            .collect(Collectors.toUnmodifiableList());

                    widgetByZ.putAll(newRecords.stream()
                            .collect(Collectors.toUnmodifiableMap(r -> uniqueKeyFactory.makeUnique(r.getZ()), r -> r)));
                }

                oldRecord.setTillTransactionId(transactionId);
                widgetById.get(id).addLast(newRecord);
                widgetByZ.put(uniqueKeyFactory.makeUnique(z), newRecord);

                int serial = transactionLog.commit(transactionId);

                logicalWriteLocks.releaseIndividualZ(z, oldRecord.getZ());
                if (isRangeLockAcquired) {
                    logicalWriteLocks.releaseRange();
                }
                if (newRecords != null) {
                    newRecords.forEach(record -> logicalWriteLocks.releaseIndividualId(record.getId()));
                }

                newRecord.setFromTransactionSerial(serial);
                oldRecord.setTillTransactionSerial(serial);
            }
            logicalWriteLocks.releaseIndividualId(id);

            return newRecord;
        });
    }

    public Widget updateWidgetByIdToMaxZ(int id, int x, int y, int height, int width) {
        return executeInGlobalLockSection(() -> {
            int transactionId = transactionIdSequence.getAndIncrement();

            logicalWriteLocks.lockIndividualId(id);
            var oldRecord = Optional.ofNullable(widgetById.get(id))
                    .flatMap(deque -> deque.isEmpty() ? Optional.empty() : Optional.of(deque.getLast()))
                    .filter(r -> recordStatus(r, Integer.MAX_VALUE) == RecordStatus.ACTIVE)
                    .orElseThrow(() -> {
                        logicalWriteLocks.releaseIndividualId(id);
                        return new ResourceNotFoundException("Widget.id=" + id);
                    });
            // essentially blocking all writes
            try {
                logicalWriteLocks.lockIndividualZWithTimeout(timeout, Integer.MIN_VALUE);
            } catch (TimeoutException e) {
                logicalWriteLocks.releaseIndividualId(id);
                throw e;
            }
            try {
                logicalWriteLocks.lockRangeWithTimeout(timeout, Integer.MIN_VALUE);
            } catch (TimeoutException e) {
                logicalWriteLocks.releaseIndividualId(id);
                logicalWriteLocks.releaseIndividualZ(Integer.MIN_VALUE);
                throw e;
            }

            // here will never be 0 since db is not empty
            int zForNewRecord = widgetByZ.descendingMap().values().stream()
                    .filter(record -> recordStatus(record, Integer.MAX_VALUE) == RecordStatus.ACTIVE)
                    .findFirst().map(r -> r.getZ() + 1).orElse(0);
            WidgetCustomImpl newRecord;
            if (oldRecord.getZ() + 1 == zForNewRecord) {
                // in this case widget is already in the foreground
                newRecord = new WidgetCustomImpl(id, x, y, oldRecord.getZ(), width, height, transactionId);
            } else {
                newRecord = new WidgetCustomImpl(id, x, y, zForNewRecord, width, height, transactionId);
            }
            if (!newRecord.equals(oldRecord)) {
                oldRecord.setTillTransactionId(transactionId);
                widgetById.get(id).addLast(newRecord);
                widgetByZ.put(uniqueKeyFactory.makeUnique(zForNewRecord), newRecord);

                int serial = transactionLog.commit(transactionId);

                newRecord.setFromTransactionSerial(serial);
                oldRecord.setTillTransactionSerial(serial);
            }

            logicalWriteLocks.releaseRange();
            logicalWriteLocks.releaseIndividualZ(Integer.MIN_VALUE);
            logicalWriteLocks.releaseIndividualId(id);

            return newRecord;
        });
    }

    @Scheduled(
            fixedRateString = "${custom-repo.vacuum-rate:60000}",
            initialDelayString = "${custom-repo.vacuum-rate:60000}")
    public void vacuum() {
        executeInGlobalLockSection(() -> {
            log.info("Running vacuum");

            int lowestRunningSerial;
            try {
                lowestRunningSerial = runningReadsSerials.first();
            } catch (NoSuchElementException exc) {
                lowestRunningSerial = transactionLog.getLatestSerial();
            }
            final int serialThreshold = lowestRunningSerial;
            var iterById = widgetById.values().iterator();
            while (iterById.hasNext()) {
                var recordsList = iterById.next();
                var iterByRecords = recordsList.iterator();
                while (iterByRecords.hasNext()) {
                    var record = iterByRecords.next();
                    Integer activeTill = getRecordTillSerial(record);
                    if (activeTill != null && activeTill <= serialThreshold) {
                        iterByRecords.remove();
                    }
                }
                if (recordsList.isEmpty()) {
                    iterById.remove();
                }
            }

            var iterByZ = widgetByZ.values().iterator();
            while (iterByZ.hasNext()) {
                var record = iterByZ.next();
                Integer activeTill = getRecordTillSerial(record);
                if (activeTill != null && activeTill <= serialThreshold) {
                    iterByZ.remove();
                }
            }
            return null;
        });
    }

    public void clear() {
        try {
            globalLock.writeLock().lockInterruptibly();
        } catch (InterruptedException e) {
            log.info("Got interrupted, exiting");
            return;
        }

        widgetById.clear();
        widgetByZ.clear();
        transactionIdSequence.set(0);
        widgetIdSequence.set(0);
        transactionLog.clear();
        logicalWriteLocks.reset();
        uniqueKeyFactory.reset();

        globalLock.writeLock().unlock();
    }

    private <T> T executeInGlobalLockSection(@NonNull Callable<T> task) {
        globalLock.readLock().lock();

        try {
            return task.call();
        } catch (TimeoutException e) {
            log.debug("Got timeout");
            throw new TimeoutRuntimeException(e);
        } catch (InterruptedException e) {
            log.info("Got interrupted, exiting");
            return null;
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            log.debug("Got checked exception");
            throw new RuntimeException("Unexpected exception", e);
        } finally {
            globalLock.readLock().unlock();
        }
    }

    @Nullable
    private WidgetCustomImpl findRecordByZ(int z, int serial) {
        var records = widgetByZ.subMap(new UniqueKey(z, Integer.MIN_VALUE), new UniqueKey(z, Integer.MAX_VALUE))
                .values().stream().filter(record -> recordStatus(record, serial) == RecordStatus.ACTIVE)
                .collect(Collectors.toUnmodifiableList());
        if (records.size() > 1) {
            log.error("Assertion failed: there was two active records with equal z={} at serial={}", z, serial);
        }
        if (records.size() == 1) {
            return records.get(0);
        }
        return null;
    }

    @Nullable
    private Integer getRecordFromSerial(@NonNull WidgetCustomImpl record) {
        Integer activeFrom = record.getFromTransactionSerial() > -1
                ? Integer.valueOf(record.getFromTransactionSerial())
                : transactionLog.getTransactionSerial(record.getFromTransactionId());
        if (activeFrom != null) {
            // caching
            record.setFromTransactionSerial(activeFrom);
        }
        return activeFrom;
    }

    @Nullable
    private Integer getRecordTillSerial(@NonNull WidgetCustomImpl record) {
        Integer activeTill = record.getTillTransactionSerial() > -1
                ? Integer.valueOf(record.getTillTransactionSerial())
                : (record.getTillTransactionId() > -1
                ? transactionLog.getTransactionSerial(record.getTillTransactionId())
                : null);
        if (activeTill != null) {
            // caching
            record.setTillTransactionSerial(activeTill);
        }
        return activeTill;
    }

    private RecordStatus recordStatus(@NonNull WidgetCustomImpl record, int serial) {
        Integer from = getRecordFromSerial(record);
        Integer till = getRecordTillSerial(record);
        if (till != null && till <= serial) {
            return RecordStatus.EXPIRED;
        }
        if (from == null || from > serial) {
            return RecordStatus.NOT_YET_COMMITTED;
        }
        return RecordStatus.ACTIVE;
    }

    private enum RecordStatus {
        EXPIRED, ACTIVE, NOT_YET_COMMITTED
    }
}
