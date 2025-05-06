package xyz.vvrf.reactor.dag.core;

import java.util.Objects;

/**
 * 代表一个类型化的节点输入槽。
 * 作为节点所需输入的标识符和类型契约。
 *
 * @param <T> 输入槽期望接收的数据类型
 * @author Refactored
 */
public final class InputSlot<T> {
    private final String id; // 在节点定义内部唯一的 ID
    private final Class<T> type;
    private final boolean required; // 标记此输入是否为执行所必需 (用于验证和可能的执行逻辑)

    private InputSlot(String id, Class<T> type, boolean required) {
        this.id = Objects.requireNonNull(id, "输入槽 ID 不能为空");
        this.type = Objects.requireNonNull(type, "输入槽类型不能为空");
        this.required = required;
    }

    /**
     * 创建一个必需的输入槽。
     */
    public static <T> InputSlot<T> required(String id, Class<T> type) {
        return new InputSlot<>(id, type, true);
    }

    /**
     * 创建一个可选的输入槽。
     */
    public static <T> InputSlot<T> optional(String id, Class<T> type) {
        return new InputSlot<>(id, type, false);
    }

    public String getId() {
        return id;
    }

    public Class<T> getType() {
        return type;
    }

    public boolean isRequired() {
        return required;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        InputSlot<?> inputSlot = (InputSlot<?>) o;
        // ID 在节点内唯一即可，不同节点的同名 ID 不应相等
        // 但这里比较困难，暂时仅比较 ID 和类型，寄希望于 ID 设计的良好实践
        return id.equals(inputSlot.id) && type.equals(inputSlot.type);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, type);
    }

    @Override
    public String toString() {
        return "InputSlot{" +
                "id='" + id + '\'' +
                ", type=" + type.getSimpleName() +
                ", required=" + required +
                '}';
    }
}