package xyz.vvrf.reactor.dag.core;

import java.util.Objects;
import java.util.UUID; // 引入 UUID

/**
 * DAG节点产生的事件接口
 *
 * @param <T> 事件数据类型
 * @author ruifeng.wen
 */
public interface Event<T> {

    /**
     * 获取事件类型
     *
     * @return 事件类型
     */
    String getEventType();

    /**
     * 获取事件ID (建议非空)
     *
     * @return 事件ID
     */
    String getId();

    /**
     * 获取事件数据
     *
     * @return 事件数据
     */
    T getData();

    /**
     * 获取事件的注释 (可选)
     *
     * @return 事件注释
     */
    default String getComment() {
        return null;
    }

    /**
     * 创建事件构建器
     *
     * @param <D> 数据类型
     * @return 事件构建器实例
     */
    static <D> Builder<D> builder() {
        return new Builder<>();
    }

    /**
     * 创建一个简单的事件实例 (自动生成 ID)
     *
     * @param <D> 数据类型
     * @param eventType 事件类型 (不能为空)
     * @param data 事件数据
     * @return 事件实例
     */
    static <D> Event<D> of(String eventType, D data) {
        return new SimpleEvent<>(
                Objects.requireNonNull(eventType, "事件类型不能为空"),
                UUID.randomUUID().toString(), // 自动生成 ID
                data,
                null);
    }

    /**
     * 创建一个带ID的事件实例
     *
     * @param <D> 数据类型
     * @param eventType 事件类型 (不能为空)
     * @param id 事件ID (不能为空)
     * @param data 事件数据
     * @return 事件实例
     */
    static <D> Event<D> of(String eventType, String id, D data) {
        return new SimpleEvent<>(
                Objects.requireNonNull(eventType, "事件类型不能为空"),
                Objects.requireNonNull(id, "事件ID不能为空"),
                data,
                null);
    }

    /**
     * 创建一个完整的事件实例
     *
     * @param <D> 数据类型
     * @param eventType 事件类型 (不能为空)
     * @param id 事件ID (不能为空)
     * @param data 事件数据
     * @param comment 事件注释
     * @return 事件实例
     */
    static <D> Event<D> of(String eventType, String id, D data, String comment) {
        return new SimpleEvent<>(
                Objects.requireNonNull(eventType, "事件类型不能为空"),
                Objects.requireNonNull(id, "事件ID不能为空"),
                data,
                comment);
    }

    /**
     * 事件构建器类
     *
     * @param <D> 数据类型
     */
    class Builder<D> {
        private String eventType;
        private String id = UUID.randomUUID().toString(); // 默认生成 ID
        private D data;
        private String comment;

        /**
         * 设置事件类型 (必须)
         *
         * @param eventType 事件类型
         * @return 构建器实例
         */
        public Builder<D> event(String eventType) {
            this.eventType = Objects.requireNonNull(eventType, "事件类型不能为空");
            return this;
        }

        /**
         * 设置事件ID (可选, 默认自动生成)
         *
         * @param id 事件ID
         * @return 构建器实例
         */
        public Builder<D> id(String id) {
            this.id = Objects.requireNonNull(id, "事件ID不能为空");
            return this;
        }

        /**
         * 设置事件数据
         *
         * @param data 事件数据
         * @return 构建器实例
         */
        public Builder<D> data(D data) {
            this.data = data;
            return this;
        }

        /**
         * 设置事件注释
         *
         * @param comment 事件注释
         * @return 构建器实例
         */
        public Builder<D> comment(String comment) {
            this.comment = comment;
            return this;
        }

        /**
         * 构建事件实例
         *
         * @return 事件实例
         * @throws NullPointerException 如果事件类型未设置
         */
        public Event<D> build() {
            Objects.requireNonNull(eventType, "构建事件前必须设置事件类型");
            return new SimpleEvent<>(eventType, id, data, comment);
        }
    }

    /**
     * 简单事件实现
     */
    class SimpleEvent<T> implements Event<T> {
        private final String eventType;
        private final String id;
        private final T data;
        private final String comment;

        // 构造函数设为包私有或私有，强制使用工厂方法或 Builder
        SimpleEvent(String eventType, String id, T data, String comment) {
            this.eventType = eventType; // 已在工厂/Builder中校验非空
            this.id = id;             // 已在工厂/Builder中校验非空
            this.data = data;
            this.comment = comment;
        }

        @Override
        public String getEventType() {
            return eventType;
        }

        @Override
        public String getId() {
            return id;
        }

        @Override
        public T getData() {
            return data;
        }

        @Override
        public String getComment() {
            return comment;
        }

        @Override
        public String toString() {
            return "Event{" +
                    "type='" + eventType + '\'' +
                    ", id='" + id + '\'' +
                    ", data=" + data +
                    (comment != null ? ", comment='" + comment + '\'' : "") +
                    '}';
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
