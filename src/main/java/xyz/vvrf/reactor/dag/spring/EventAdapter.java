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
     * 将可能包含混合类型的 Event<?> 流转换为 Spring 的 ServerSentEvent<?> 流。
     *
     * @param events 源事件流 (Flux<Event<?>>)
     * @return 转换后的 ServerSentEvent 流 (Flux<ServerSentEvent<?>>)
     */
    @SuppressWarnings({"unchecked", "rawtypes"}) // 抑制泛型转换警告
    public static Flux<ServerSentEvent<?>> toServerSentEvents(Flux<Event<?>> events) {
        return events.map(event -> {
            // Builder 的类型会根据 data() 参数的类型自动推断
            ServerSentEvent.Builder builder = ServerSentEvent.builder()
                    .data(event.getData()); // data 是 Object 类型

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
     * 将 Spring 的 ServerSentEvent<?> 流转换为 Event<?> 流。
     * 如果未指定，假定来自 SSE 的数据类型是 Object。
     *
     * @param serverSentEvents 源 ServerSentEvent 流 (Flux<ServerSentEvent<?>>)
     * @return 转换后的 Event 流 (Flux<Event<?>>)
     */
    public static Flux<Event<?>> fromServerSentEvents(Flux<ServerSentEvent<?>> serverSentEvents) {
        return serverSentEvents.map(sse ->
                // 如果知道数据类型，可以使用泛型的 Event.Builder 以确保类型安全。
                // 否则，直接创建 SimpleEvent 或在必要时强制转换数据。
                // 这里我们假设数据是 Object。
                Event.of(
                        sse.event() != null ? sse.event() : "message", // SSE 默认事件类型是 "message"
                        sse.id(),
                        sse.data(), // data 是 Object 类型
                        sse.comment()
                )
        );
    }
}
