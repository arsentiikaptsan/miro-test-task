package ru.miro.hr.task.service;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import ru.miro.hr.task.model.Widget;
import ru.miro.hr.task.rest.dto.WidgetDto;

public interface WidgetService {

    Mono<Widget> getWidget(int id);

    Flux<Widget> getWidgets(int from, int size);

    Mono<Integer> getWidgetsCount();

    Mono<Widget> createWidget(WidgetDto dto);

    Mono<Widget> updateWidget(WidgetDto dto);

    Mono deleteWidget(int id);
}
