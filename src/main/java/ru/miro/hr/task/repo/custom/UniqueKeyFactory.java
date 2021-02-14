package ru.miro.hr.task.repo.custom;

import java.util.concurrent.atomic.AtomicInteger;

class UniqueKeyFactory {

    private final AtomicInteger uniqueSequence = new AtomicInteger(0);

    UniqueKey makeUnique(int key) {
        return new UniqueKey(key, uniqueSequence.getAndIncrement());
    }

    void reset() {
        uniqueSequence.set(0);
    }
}
