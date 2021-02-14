package ru.miro.hr.task;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.test.web.reactive.server.WebTestClient;
import reactor.core.publisher.Mono;
import ru.miro.hr.task.model.Widget;
import ru.miro.hr.task.rest.dto.WidgetDto;

import java.util.Random;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

public abstract class IT {

    private final Random random = new Random();

    @Autowired
    WebTestClient client;

    @Test
    public void testCreateAndGetWidgetRequests() {
        Widget widget = createWidget();

        Widget widgetCpy = getWidget(widget.getId());

        assertThat(widgetCpy).isEqualTo(widget);
    }

    @Test
    public void testCreateRequestWithInvalidDto() {
        var invalidDto = randomWidgetDto(null);
        invalidDto.setHeight(-1);
        client.post().uri(uriBuilder -> uriBuilder
                .path("/widget")
                .queryParam("tid", "{tid}")
                .build(UUID.randomUUID().toString()))
                .body(Mono.just(invalidDto), WidgetDto.class)
                .exchange()
                .expectStatus().isBadRequest();
    }

    @Test
    public void testIdempotenceOfCreateRequest() {
        String tid = UUID.randomUUID().toString();
        client.post().uri(uriBuilder -> uriBuilder
                .path("/widget")
                .queryParam("tid", "{tid}")
                .build(tid))
                .body(Mono.just(randomWidgetDto(null)), WidgetDto.class)
                .exchange()
                .expectStatus().is2xxSuccessful();

        client.post().uri(uriBuilder -> uriBuilder
                .path("/widget")
                .queryParam("tid", "{tid}")
                .build(tid))
                .body(Mono.just(randomWidgetDto(null)), WidgetDto.class)
                .exchange()
                .expectStatus().isEqualTo(HttpStatus.CONFLICT);
    }

    @Test
    public void testCreateRequestWithoutSpecifiedZ() {
        Widget widget1 = createWidget((Integer) null);
        assertThat(widget1.getZ()).isEqualTo(0);

        Widget widget2 = createWidget((Integer) null);
        assertThat(widget2.getZ()).isEqualTo(1);
    }

    @Test
    public void testCreateRequestWithExistingZ() {
        Widget widget1 = createWidget(0);
        Widget widget2 = createWidget(1);

        Widget widget0 = createWidget(0);

        widget1 = getWidget(widget1.getId());
        widget2 = getWidget(widget2.getId());

        assertThat(widget0.getZ()).isEqualTo(0);
        assertThat(widget1.getZ()).isEqualTo(1);
        assertThat(widget2.getZ()).isEqualTo(2);
    }

    @Test
    public void testPageableGetRequest() {
        int id1 = createWidget(1).getId();
        int id2 = createWidget(2).getId();
        int id3 = createWidget(3).getId();

        assertThat(countWidgets()).isEqualTo(3);

        client.get().uri(uriBuilder -> uriBuilder
                .path("/widget")
                .queryParam("from", Integer.MIN_VALUE)
                .queryParam("size", 2)
                .build())
                .exchange()
                .expectStatus().isOk()
                .expectBodyList(WidgetImpl.class)
                .hasSize(2)
                .value(widgets -> assertThat(widgets).extracting("id").containsExactly(id1, id2));
    }

    @Test
    public void testInvalidPageableGetRequest() {
        client.get().uri(uriBuilder -> uriBuilder
                .path("/widget")
                .queryParam("from", Integer.MIN_VALUE)
                .queryParam("size", 1000)
                .build())
                .exchange()
                .expectStatus().isBadRequest();
    }

    @Test
    public void testUpdateRequestOnNonExistentId() {
        client.put().uri("/widget/{id}", 1)
                .body(Mono.just(randomWidgetDto(1)), WidgetDto.class)
                .exchange()
                .expectStatus().isNotFound();
    }

    @Test
    public void testUpdateRequestWithIdenticalData() {
        var dto = randomWidgetDto(null);
        WidgetImpl createdWidget = createWidget(dto);

        client.put().uri("/widget/{id}", createdWidget.getId())
                .body(Mono.just(dto), WidgetDto.class)
                .exchange()
                .expectStatus().isOk()
                .expectBody(WidgetImpl.class).isEqualTo(createdWidget);
    }

    @Test
    public void testUpdateRequestWithIdenticalDataButNoZSpecifiedAndWidgetInForeground() {
        createWidget(1);

        var dto = randomWidgetDto(null);
        dto.setZ(2);
        var createdWidget = createWidget(dto);

        dto.setZ(null);
        dto.setX(5);
        createdWidget.setX(5);

        client.put().uri("/widget/{id}", createdWidget.getId())
                .body(Mono.just(dto), WidgetDto.class)
                .exchange()
                .expectStatus().isOk()
                .expectBody(WidgetImpl.class).isEqualTo(createdWidget);
    }

    @Test
    public void testUpdateRequestWithIdenticalDataButNoZSpecifiedAndWidgetNotInForeground() {
        createWidget(2);

        var dto = randomWidgetDto(null);
        dto.setZ(1);
        var createdWidget = createWidget(dto);

        dto.setZ(null);
        createdWidget.setZ(3);

        client.put().uri("/widget/{id}", createdWidget.getId())
                .body(Mono.just(dto), WidgetDto.class)
                .exchange()
                .expectStatus().isOk()
                .expectBody(WidgetImpl.class).isEqualTo(createdWidget);
    }

    @Test
    public void testUpdateRequestWithExistingZ() {
        Widget widget1 = createWidget(1);
        Widget widget2 = createWidget(2);
        Widget widget3 = createWidget(3);

        var dto = randomWidgetDto(widget1.getId());
        dto.setZ(2);

        client.put().uri("/widget/{id}", widget1.getId())
                .body(Mono.just(dto), WidgetDto.class)
                .exchange()
                .expectStatus().isOk()
                .expectBody(WidgetImpl.class).value(w -> assertThat(w.getZ()).isEqualTo(2));

        widget2 = getWidget(widget2.getId());
        widget3 = getWidget(widget3.getId());

        assertThat(widget2.getZ()).isEqualTo(3);
        assertThat(widget3.getZ()).isEqualTo(4);
    }

    @Test
    public void testDeleteRequest() {
        createWidget();
        int id = createWidget().getId();

        deleteWidget(id);
        assertThat(countWidgets()).isEqualTo(1);
        checkNoWidgetExistWithId(id);
    }

    @Test
    public void testDeleteRequestOnNonExistentId() {
        client.delete().uri("/widget/{id}", 1)
                .exchange()
                .expectStatus().isNotFound();
    }

    @SuppressWarnings("ConstantConditions")
    private int countWidgets() {
        return client.get().uri("/widget_count")
                .exchange()
                .expectStatus().is2xxSuccessful()
                .returnResult(Integer.class)
                .getResponseBody().blockFirst();
    }

    private void deleteWidget(int id) {
        client.delete().uri("/widget/{id}", id)
                .exchange()
                .expectStatus().isOk()
                .expectBody().isEmpty();
    }

    private WidgetImpl createWidget(Integer z) {
        var dto = new WidgetDto(
                null,
                random.nextInt(),
                random.nextInt(),
                z,
                random.nextInt(1000) + 1,
                random.nextInt(1000) + 1);
        return createWidget(dto);
    }

    private WidgetImpl createWidget(WidgetDto dto) {
        return client.post().uri(uriBuilder -> uriBuilder
                .path("/widget")
                .queryParam("tid", "{tid}")
                .build(UUID.randomUUID().toString()))
                .body(Mono.just(dto), WidgetDto.class)
                .attribute("tid", UUID.randomUUID().toString())
                .exchange()
                .expectStatus().isCreated()
                .expectBody(WidgetImpl.class).value(w -> {
                    assertThat(isWidgetValid(w)).isTrue();
                    assertThat(w.getX()).isEqualTo(dto.getX());
                    assertThat(w.getY()).isEqualTo(dto.getY());
                    assertThat(w.getHeight()).isEqualTo(dto.getHeight());
                    assertThat(w.getWidth()).isEqualTo(dto.getWidth());

                })
                .returnResult().getResponseBody();
    }

    private WidgetImpl createWidget() {
        return createWidget(random.nextInt());
    }

    private WidgetImpl getWidget(int id) {
        return client.get().uri("/widget/{id}", id)
                .exchange()
                .expectStatus().is2xxSuccessful()
                .returnResult(WidgetImpl.class)
                .getResponseBody().blockFirst();
    }

    private void checkNoWidgetExistWithId(int id) {
        client.get().uri("/widget/{id}", id)
                .exchange()
                .expectStatus().isNotFound();
    }

    private boolean isWidgetValid(Widget widget) {
        return widget.getWidth() > 0 && widget.getHeight() > 0;
    }

    private WidgetDto randomWidgetDto(Integer id) {
        return new WidgetDto(
                id,
                random.nextInt(),
                random.nextInt(),
                random.nextInt(),
                random.nextInt(1000) + 1,
                random.nextInt(1000) + 1);
    }
}
