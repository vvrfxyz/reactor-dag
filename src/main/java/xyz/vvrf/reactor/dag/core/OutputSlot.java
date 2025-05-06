package xyz.vvrf.reactor.dag.core;

import java.util.Objects;

/**
 * 代表一个类型化的节点输出槽。
 * 通常一个节点只有一个主输出，但允许定义多个。
 *
 * @param <T> 输出槽产生的数据类型
 * @author Refactored
 */
public final class OutputSlot<T> {
    // 节点的默认/主输出槽的约定 ID
    public static final String DEFAULT_OUTPUT_SLOT_ID = "_default_output";

    private final String id; // 在节点定义内部唯一的 ID
    private final Class<T> type;

    private OutputSlot(String id, Class<T> type) {
        this.id = Objects.requireNonNull(id, "输出槽 ID 不能为空");
        this.type = Objects.requireNonNull(type, "输出槽类型不能为空");
    }

    /**
     * 创建一个输出槽。
     */
    public static <T> OutputSlot<T> create(String id, Class<T> type) {
        return new OutputSlot<>(id, type);
    }

    /**
     * 创建节点的默认输出槽。
     */
    public static <T> OutputSlot<T> defaultOutput(Class<T> type) {
        return new OutputSlot<>(DEFAULT_OUTPUT_SLOT_ID, type);
    }

    public String getId() {
        return id;
    }

    public Class<T> getType() {
        return type;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        OutputSlot<?> that = (OutputSlot<?>) o;
        // 同样，ID 在节点内唯一
        return id.equals(that.id) && type.equals(that.type);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, type);
    }

    @Override
    public String toString() {
        return "OutputSlot{" +
                "id='" + id + '\'' +
                ", type=" + type.getSimpleName() +
                '}';
    }
}