package xyz.vvrf.reactor.dag.core;

/**
 * 描述 DagNode 对输入数据的需求。
 *
 * @param <T> 期望的输入 Payload 类型
 * @author ruifeng.wen
 * @date 4/25/25
 */

import java.util.Objects;
import java.util.Optional;

public final class InputRequirement<T> {
    private final Class<T> type;
    private final String qualifier;
    private final boolean optional;

    private InputRequirement(Class<T> type, String qualifier, boolean optional) {
        this.type = Objects.requireNonNull(type, "Input type cannot be null");
        this.qualifier = qualifier;
        this.optional = optional;
    }

    /**
     * 创建一个必需的输入需求 (无限定符)。
     * @param type 期望的 Payload 类型
     */
    public static <T> InputRequirement<T> require(Class<T> type) {
        return new InputRequirement<>(type, null, false);
    }

    /**
     * 创建一个必需的输入需求 (带限定符)。
     * @param type 期望的 Payload 类型
     * @param qualifier 限定符
     */
    public static <T> InputRequirement<T> require(Class<T> type, String qualifier) {
        Objects.requireNonNull(qualifier, "Qualifier cannot be null for required input with qualifier");
        return new InputRequirement<>(type, qualifier, false);
    }

    /**
     * 创建一个可选的输入需求 (无限定符)。
     * @param type 期望的 Payload 类型
     */
    public static <T> InputRequirement<T> optional(Class<T> type) {
        return new InputRequirement<>(type, null, true);
    }

    /**
     * 创建一个可选的输入需求 (带限定符)。
     * @param type 期望的 Payload 类型
     * @param qualifier 限定符
     */
    public static <T> InputRequirement<T> optional(Class<T> type, String qualifier) {
        Objects.requireNonNull(qualifier, "Qualifier cannot be null for optional input with qualifier");
        return new InputRequirement<>(type, qualifier, true);
    }

    public Class<T> getType() {
        return type;
    }

    public Optional<String> getQualifier() {
        return Optional.ofNullable(qualifier);
    }

    public boolean isOptional() {
        return optional;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        InputRequirement<?> that = (InputRequirement<?>) o;
        return type.equals(that.type) && Objects.equals(qualifier, that.qualifier);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, qualifier);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(optional ? "OptionalInput" : "RequiredInput");
        sb.append("{type=").append(type.getSimpleName());
        if (qualifier != null) {
            sb.append(", qualifier='").append(qualifier).append('\'');
        }
        sb.append('}');
        return sb.toString();
    }
}
