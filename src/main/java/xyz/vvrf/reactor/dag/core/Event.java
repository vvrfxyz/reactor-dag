package xyz.vvrf.reactor.dag.core;

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
     * 获取事件ID
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
     * 获取事件的注释
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
     * 创建一个简单的事件实例
     *
     * @param <D> 数据类型
     * @param eventType 事件类型
     * @param data 事件数据
     * @return 事件实例
     */
    static <D> Event<D> of(String eventType, D data) {
        return new SimpleEvent<>(eventType, null, data, null);
    }

    /**
     * 创建一个带ID的事件实例
     *
     * @param <D> 数据类型
     * @param eventType 事件类型
     * @param id 事件ID
     * @param data 事件数据
     * @return 事件实例
     */
    static <D> Event<D> of(String eventType, String id, D data) {
        return new SimpleEvent<>(eventType, id, data, null);
    }

    /**
     * 创建一个完整的事件实例
     *
     * @param <D> 数据类型
     * @param eventType 事件类型
     * @param id 事件ID
     * @param data 事件数据
     * @param comment 事件注释
     * @return 事件实例
     */
    static <D> Event<D> of(String eventType, String id, D data, String comment) {
        return new SimpleEvent<>(eventType, id, data, comment);
    }

    /**
     * 事件构建器类
     *
     * @param <D> 数据类型
     */
    class Builder<D> {
        private String eventType;
        private String id;
        private D data;
        private String comment;

        /**
         * 设置事件类型
         *
         * @param eventType 事件类型
         * @return 构建器实例
         */
        public Builder<D> event(String eventType) {
            this.eventType = eventType;
            return this;
        }

        /**
         * 设置事件ID
         *
         * @param id 事件ID
         * @return 构建器实例
         */
        public Builder<D> id(String id) {
            this.id = id;
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
         */
        public Event<D> build() {
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

        SimpleEvent(String eventType, String id, T data, String comment) {
            this.eventType = eventType;
            this.id = id;
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
    }
}