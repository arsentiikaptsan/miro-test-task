package ru.miro.hr.task.model;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;

@JsonSerialize(as = Widget.class)
public interface Widget {

    int getId();

    int getX();

    int getY();

    int getZ();

    int getWidth();

    int getHeight();
}
