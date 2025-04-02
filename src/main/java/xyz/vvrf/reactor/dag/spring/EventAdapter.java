package xyz.vvrf.reactor.dag.spring;

import org.springframework.http.codec.ServerSentEvent;
import reactor.core.publisher.Flux;
import xyz.vvrf.reactor.dag.core.Event;

/**
 * 事件适配器 - 在核心Event接口和Spring的ServerSentEvent之间转换
 *
 * @author ruifeng.wen
 */
public final class EventAdapter {

    private EventAdapter() {
        // 工具类不允许实例化
    }

    /**
     * 将Event流转换为Spring的ServerSentEvent流
     *
     * @param <T> 事件数据类型
     * @param events 源事件流
     * @return 转换后的ServerSentEvent流
     */
    public static <T> Flux<ServerSentEvent<T>> toServerSentEvents(Flux<Event<T>> events) {
        return events.map(event -> {
            ServerSentEvent.Builder<T> builder = ServerSentEvent.<T>builder()
                    .data(event.getData());

            if (event.getEventType() != null) {
                builder.event(event.getEventType());
            }

            if (event.getId() != null) {
                builder.id(event.getId());
            }

            if (event.getComment() != null) {
                builder.comment(event.getComment());
            }

            return builder.build();
        });
    }

    /**
     * 将Spring的ServerSentEvent流转换为Event流
     *
     * @param <T> 事件数据类型
     * @param serverSentEvents 源ServerSentEvent流
     * @return 转换后的Event流
     */
    public static <T> Flux<Event<T>> fromServerSentEvents(Flux<ServerSentEvent<T>> serverSentEvents) {
        return serverSentEvents.map(sse ->
                Event.of(
                        sse.event() != null ? sse.event() : "message",
                        sse.id(),
                        sse.data(),
                        sse.comment()
                )
        );
    }
}