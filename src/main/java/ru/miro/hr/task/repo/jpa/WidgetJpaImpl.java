package ru.miro.hr.task.repo.jpa;

import lombok.AccessLevel;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.Setter;
import ru.miro.hr.task.model.Widget;

import javax.persistence.*;

@Entity
@Table(name = "widget", indexes = @Index(name = "widget_unique_z_index", columnList = "z", unique = true))
@Data
@EqualsAndHashCode(onlyExplicitlyIncluded = true)
public class WidgetJpaImpl implements Widget {

    @Id
    @GeneratedValue
    @EqualsAndHashCode.Include
    @Setter(AccessLevel.NONE)
    private int id;

    // (x, y) - are coordinates of the left bottom corner
    private int x, y, z, width, height;

    public WidgetJpaImpl(int x, int y, int z, int width, int height) {
        this.x = x;
        this.y = y;
        this.z = z;
        this.width = width;
        this.height = height;
    }

    public WidgetJpaImpl() {
    }
}
