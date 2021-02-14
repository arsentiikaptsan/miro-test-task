package ru.miro.hr.task.repo.custom;

import lombok.Value;

@Value
class UniqueKey implements Comparable<UniqueKey> {

    int key;
    int uniqueAddOn;

    @Override
    public int compareTo(UniqueKey that) {
        int delta = Integer.compare(this.key, that.key);
        if (delta != 0) {
            return delta;
        }
        return Integer.compare(this.uniqueAddOn, that.uniqueAddOn);
    }
}
