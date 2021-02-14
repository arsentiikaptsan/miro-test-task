package ru.miro.hr.task.rest.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class WidgetDto {

    private Integer id, x, y, z, width, height;

    public boolean isValid() {
        return x != null && y != null && width != null && height != null && width > 0 && height > 0;
    }
}
