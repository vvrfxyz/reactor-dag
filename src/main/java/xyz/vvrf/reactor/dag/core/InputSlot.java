package xyz.vvrf.reactor.dag.core;

import java.util.Objects;

/**
 * 代表一个类型化的节点输入槽（不可变数据类）。
 * 作为节点所需输入的标识符和类型契约。
 *
 * @param <T> 输入槽期望接收的数据类型
 * @author Refactored (注释更新)
 */
public final class InputSlot<T> {
    private final String id; // 在节点定义内部唯一的 ID
    private final Class<T> type;
    private final boolean required; // 标记此输入是否为执行所必需 (主要用于图构建时验证)

    private InputSlot(String id, Class<T> type, boolean required) {
        this.id = Objects.requireNonNull(id, "输入槽 ID 不能为空");
        this.type = Objects.requireNonNull(type, "输入槽类型不能为空");
        this.required = required;
    }

    /**
     * 创建一个必需的输入槽。
     * 必需的输入槽在图构建时会验证是否至少有一条边连接到它。
     *
     * @param <T>  数据类型
     * @param id   输入槽 ID (在节点内唯一)
     * @param type 期望的数据类型
     * @return 新的 InputSlot 实例
     */
    public static <T> InputSlot<T> required(String id, Class<T> type) {
        return new InputSlot<>(id, type, true);
    }

    /**
     * 创建一个可选的输入槽。
     * 可选输入槽不强制要求有边连接。节点逻辑需要处理其可能不存在的情况。
     *
     * @param <T>  数据类型
     * @param id   输入槽 ID (在节点内唯一)
     * @param type 期望的数据类型
     * @return 新的 InputSlot 实例
     */
    public static <T> InputSlot<T> optional(String id, Class<T> type) {
        return new InputSlot<>(id, type, false);
    }

    // --- Getters ---

    public String getId() {
        return id;
    }

    public Class<T> getType() {
        return type;
    }

    public boolean isRequired() {
        return required;
    }

    // --- equals, hashCode, toString ---

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        InputSlot<?> inputSlot = (InputSlot<?>) o;
        // 比较 ID 和类型。ID 在节点内部是唯一的，不同节点的同名 ID 不应视为相等。
        // 但 InputSlot 对象通常在节点实例内部创建和比较，所以比较 ID 和类型通常足够。
        return id.equals(inputSlot.id) && type.equals(inputSlot.type);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, type);
    }

    @Override
    public String toString() {
        return String.format("InputSlot[id=%s, type=%s, required=%b]",
                id, type.getSimpleName(), required);
    }
}
