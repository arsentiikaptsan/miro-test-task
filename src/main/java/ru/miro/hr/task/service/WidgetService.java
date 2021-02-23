package ru.miro.hr.task.service;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import ru.miro.hr.task.model.Widget;
import ru.miro.hr.task.rest.dto.WidgetDto;

public interface WidgetService {

    Mono<? extends Widget> getWidget(int id);

    Flux<? extends Widget> getWidgets(int from, int size);

    Mono<Integer> getWidgetsCount();

    Mono<? extends Widget> createWidget(WidgetDto dto);

    Mono<? extends Widget> updateWidget(WidgetDto dto);

    Mono<Void> deleteWidget(int id);
}
