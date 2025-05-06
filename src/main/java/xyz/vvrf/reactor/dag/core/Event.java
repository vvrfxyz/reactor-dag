package xyz.vvrf.reactor.dag.core;

import java.util.Objects;
import java.util.UUID;

/**
 * 代表 DAG 节点执行过程中产生的事件。
 * 事件应为不可变对象。
 *
 * @param <T> 事件负载 (payload) 的类型
 * @author Refactored (注释更新)
 */
public interface Event<T> {

    /**
     * 获取事件类型。建议使用有意义的字符串，如 "OrderCreated", "PaymentProcessed"。
     *
     * @return 事件类型 (不能为空)
     */
    String getEventType();

    /**
     * 获取事件的唯一标识符。
     *
     * @return 事件 ID (不能为空)
     */
    String getId();

    /**
     * 获取事件的负载数据。
     *
     * @return 事件数据 (可以为 null)
     */
    T getData();

    /**
     * 获取事件的注释信息 (可选)。
     *
     * @return 事件注释，或 null
     */
    default String getComment() {
        return null;
    }

    // --- 工厂方法和构建器 ---

    /**
     * 创建一个简单的事件实例，自动生成 UUID 作为 ID。
     *
     * @param <D>       数据类型
     * @param eventType 事件类型 (不能为空)
     * @param data      事件数据
     * @return 新的 Event 实例
     */
    static <D> Event<D> of(String eventType, D data) {
        return new SimpleEvent<>(
                Objects.requireNonNull(eventType, "事件类型不能为空"),
                UUID.randomUUID().toString(),
                data,
                null);
    }

    /**
     * 创建一个带有指定 ID 的事件实例。
     *
     * @param <D>       数据类型
     * @param eventType 事件类型 (不能为空)
     * @param id        事件 ID (不能为空)
     * @param data      事件数据
     * @return 新的 Event 实例
     */
    static <D> Event<D> of(String eventType, String id, D data) {
        return new SimpleEvent<>(
                Objects.requireNonNull(eventType, "事件类型不能为空"),
                Objects.requireNonNull(id, "事件ID不能为空"),
                data,
                null);
    }

    /**
     * 创建一个包含所有字段的事件实例。
     *
     * @param <D>       数据类型
     * @param eventType 事件类型 (不能为空)
     * @param id        事件 ID (不能为空)
     * @param data      事件数据
     * @param comment   事件注释
     * @return 新的 Event 实例
     */
    static <D> Event<D> of(String eventType, String id, D data, String comment) {
        return new SimpleEvent<>(
                Objects.requireNonNull(eventType, "事件类型不能为空"),
                Objects.requireNonNull(id, "事件ID不能为空"),
                data,
                comment);
    }

    /**
     * 获取事件构建器。
     *
     * @param <D> 数据类型
     * @return 事件构建器实例
     */
    static <D> Builder<D> builder() {
        return new Builder<>();
    }

    /**
     * 事件构建器类。
     *
     * @param <D> 数据类型
     */
    class Builder<D> {
        private String eventType;
        private String id = UUID.randomUUID().toString(); // 默认生成 ID
        private D data;
        private String comment;

        /**
         * 设置事件类型 (必须)。
         * @param eventType 事件类型
         * @return this builder
         */
        public Builder<D> event(String eventType) {
            this.eventType = Objects.requireNonNull(eventType, "事件类型不能为空");
            return this;
        }

        /**
         * 设置事件 ID (可选, 默认自动生成)。
         * @param id 事件 ID
         * @return this builder
         */
        public Builder<D> id(String id) {
            this.id = Objects.requireNonNull(id, "事件ID不能为空");
            return this;
        }

        /**
         * 设置事件数据。
         * @param data 事件数据
         * @return this builder
         */
        public Builder<D> data(D data) {
            this.data = data;
            return this;
        }

        /**
         * 设置事件注释。
         * @param comment 事件注释
         * @return this builder
         */
        public Builder<D> comment(String comment) {
            this.comment = comment;
            return this;
        }

        /**
         * 构建事件实例。
         *
         * @return 构建好的 Event 实例
         * @throws NullPointerException 如果事件类型未设置
         */
        public Event<D> build() {
            Objects.requireNonNull(eventType, "构建事件前必须设置事件类型");
            return new SimpleEvent<>(eventType, id, data, comment);
        }
    }

    /**
     * Event 接口的简单不可变实现。
     */
    class SimpleEvent<T> implements Event<T> {
        private final String eventType;
        private final String id;
        private final T data;
        private final String comment;

        // 包私有构造函数，强制使用工厂方法或 Builder
        SimpleEvent(String eventType, String id, T data, String comment) {
            this.eventType = eventType;
            this.id = id;
            this.data = data;
            this.comment = comment;
        }

        @Override public String getEventType() { return eventType; }
        @Override public String getId() { return id; }
        @Override public T getData() { return data; }
        @Override public String getComment() { return comment; }

        @Override
        public String toString() {
            return String.format("Event[type=%s, id=%s, data=%s%s]",
                    eventType, id, data,
                    (comment != null ? ", comment='" + comment + "'" : ""));
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            SimpleEvent<?> that = (SimpleEvent<?>) o;
            return eventType.equals(that.eventType) &&
                    id.equals(that.id) &&
                    Objects.equals(data, that.data) &&
                    Objects.equals(comment, that.comment);
        }

        @Override
        public int hashCode() {
            return Objects.hash(eventType, id, data, comment);
        }
    }
}
