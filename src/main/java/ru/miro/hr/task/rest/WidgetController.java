package ru.miro.hr.task.rest;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.server.reactive.ServerHttpResponse;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import ru.miro.hr.task.exception.BadRequestException;
import ru.miro.hr.task.exception.DublicateException;
import ru.miro.hr.task.model.Widget;
import ru.miro.hr.task.rest.dto.WidgetDto;
import ru.miro.hr.task.service.WidgetService;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

@RestController
public class WidgetController {

    private final static String MIN_INT_STRING = "-2147483648";

    private final WidgetService service;

    private final Set<String> processedTId;

    @Autowired
    public WidgetController(WidgetService service,
                            @Value("${controller.tid-set-init-size:1000}") int initialSize) {
        this.service = service;
        processedTId = ConcurrentHashMap.newKeySet(initialSize);
    }

    @GetMapping(value = "/widget/{id}")
    Mono<Widget> getWidget(@PathVariable("id") int id) {
        return service.getWidget(id);
    }

    /**
     * I use unorthodox approach to pagination: instead of offset(page_number) and page_size there are greater-than-value and page_size.
     * This way (if there is an index on column, and we have one) query complexity does not grow with a page_number.
     * Instead of {page=0,size=10} we use {from=-inf,size=10}, which results in widgets with z from -inf to,
     * let's say, 10. Then to get the next page we set: {from=11,page=10}.
     */
    @GetMapping("/widget")
    Flux<Widget> getWidgets(@RequestParam(name = "from", defaultValue = MIN_INT_STRING) int from,
                            @RequestParam(name = "size", defaultValue = "10") int size,
                            ServerHttpResponse response) {
        if (size <= 0 || size > 500) {
            throw new BadRequestException();
        }
        return service.getWidgets(from, size);
    }

    /**
     * Counting query can be significantly longer and more expensive to execute than a single page request.
     * For this reason there is a separate endpoint for counting, unlike a common approach, where size of
     * a result set is unnecessarily calculated for every page request.
     */
    @GetMapping("/widget_count")
    Mono<Integer> getWidgetsCount() {
        return service.getWidgetsCount();
    }

    /**
     * We introduce tid to make this endpoint idempotent.
     *
     * @param tid - client-generated transaction id (UUID for example, but can be smaller)
     * @return created widget
     */
    @PostMapping("/widget")
    @ResponseStatus(HttpStatus.CREATED)
    Mono<Widget> createWidget(@RequestParam(name = "tid") String tid,
                              @RequestBody WidgetDto dto) {
        if (dto.getId() != null || !dto.isValid()) {
            throw new BadRequestException();
        }

        if (!processedTId.add(tid)) {
            throw new DublicateException();
        }

        return service.createWidget(dto);
    }

    @PutMapping("/widget/{id}")
    Mono<Widget> updateWidget(@PathVariable(name = "id") int id,
                              @RequestBody WidgetDto dto) {
        if (!dto.isValid()) {
            throw new BadRequestException();
        }

        dto.setId(id);
        return service.updateWidget(dto);
    }

    @DeleteMapping("/widget/{id}")
    Mono deleteWidget(@PathVariable(name = "id") int id) {
        return service.deleteWidget(id);
    }
}
