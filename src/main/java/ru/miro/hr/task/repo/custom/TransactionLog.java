package ru.miro.hr.task.repo.custom;

import org.springframework.lang.Nullable;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class TransactionLog {

    private final Map<Integer, Integer> transactionSerials;
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private int serial = 0;

    TransactionLog(int initialCapacity) {
        this.transactionSerials = new HashMap<>(initialCapacity);
    }

    int commit(int transactionId) {
        lock.writeLock().lock();

        if (transactionSerials.containsKey(transactionId)) {
            throw new IllegalStateException("Already committed");
        }
        transactionSerials.put(transactionId, serial);
        int result = serial++;

        lock.writeLock().unlock();
        return result;
    }

    int getLatestSerial() {
        lock.readLock().lock();
        int result = serial - 1;
        lock.readLock().unlock();
        return result;
    }

    @Nullable
    Integer getTransactionSerial(int transactionId) {
        lock.readLock().lock();
        Integer result = transactionSerials.get(transactionId);
        lock.readLock().unlock();
        return result;
    }

    void clear() {
        lock.writeLock().lock();
        serial = 0;
        transactionSerials.clear();
        lock.writeLock().unlock();
    }
}
