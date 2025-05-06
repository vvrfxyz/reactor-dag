package xyz.vvrf.reactor.dag.core;

import java.util.Objects;

/**
 * 代表一个类型化的节点输出槽（不可变数据类）。
 * 通常一个节点只有一个主输出（默认输出），但允许定义多个命名输出槽。
 *
 * @param <T> 输出槽产生的数据类型
 * @author Refactored (注释更新)
 */
public final class OutputSlot<T> {
    /**
     * 节点的默认/主输出槽的约定 ID。
     * 当节点只有一个输出时，通常使用此 ID。
     */
    public static final String DEFAULT_OUTPUT_SLOT_ID = "_default_output";

    private final String id; // 在节点定义内部唯一的 ID
    private final Class<T> type;

    private OutputSlot(String id, Class<T> type) {
        this.id = Objects.requireNonNull(id, "输出槽 ID 不能为空");
        this.type = Objects.requireNonNull(type, "输出槽类型不能为空");
    }

    /**
     * 创建一个命名输出槽。
     *
     * @param <T>  数据类型
     * @param id   输出槽 ID (在节点内唯一, 不能是 {@value #DEFAULT_OUTPUT_SLOT_ID}，除非明确创建默认槽)
     * @param type 产生的数据类型
     * @return 新的 OutputSlot 实例
     */
    public static <T> OutputSlot<T> create(String id, Class<T> type) {
        if (DEFAULT_OUTPUT_SLOT_ID.equals(id)) {
            // 或者抛出异常，或者允许但建议使用 defaultOutput()
            System.err.println("警告: 使用 create() 创建了 ID 为 '" + DEFAULT_OUTPUT_SLOT_ID + "' 的输出槽，建议使用 defaultOutput() 方法。");
        }
        return new OutputSlot<>(id, type);
    }

    /**
     * 创建节点的默认输出槽。
     *
     * @param <T>  数据类型
     * @param type 产生的数据类型
     * @return 代表默认输出的 OutputSlot 实例
     */
    public static <T> OutputSlot<T> defaultOutput(Class<T> type) {
        return new OutputSlot<>(DEFAULT_OUTPUT_SLOT_ID, type);
    }

    // --- Getters ---

    public String getId() {
        return id;
    }

    public Class<T> getType() {
        return type;
    }

    // --- equals, hashCode, toString ---

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        OutputSlot<?> that = (OutputSlot<?>) o;
        // 比较 ID 和类型。
        return id.equals(that.id) && type.equals(that.type);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, type);
    }

    @Override
    public String toString() {
        return String.format("OutputSlot[id=%s, type=%s]",
                id, type.getSimpleName());
    }
}
