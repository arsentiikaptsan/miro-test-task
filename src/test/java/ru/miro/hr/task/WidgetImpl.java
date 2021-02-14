package ru.miro.hr.task;

import lombok.Data;
import ru.miro.hr.task.model.Widget;

@Data
public class WidgetImpl implements Widget {

    private int id, x, y, z, width, height;
}
