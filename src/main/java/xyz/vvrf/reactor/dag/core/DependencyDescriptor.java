package xyz.vvrf.reactor.dag.core;

import java.util.Objects;

/**
 * 描述一个 DAG 节点的依赖项，包括名称和期望的输入类型 (Payload Type)。
 *
 * @author ruifeng.wen
 */
public final class DependencyDescriptor {
    private final String name;
    private final Class<?> requiredType; // 这个类型指的是依赖节点需要提供的 Payload 类型

    /**
     * 创建依赖描述符
     *
     * @param name 依赖节点的名称
     * @param requiredType 依赖节点需要输出的 Payload 类型
     * @throws NullPointerException 如果名称或类型为null
     */
    public DependencyDescriptor(String name, Class<?> requiredType) {
        this.name = Objects.requireNonNull(name, "依赖名称不能为空");
        this.requiredType = Objects.requireNonNull(requiredType, "依赖类型不能为空");
    }

    /**
     * 获取依赖节点名称
     *
     * @return 依赖节点名称
     */
    public String getName() {
        return name;
    }

    /**
     * 获取依赖节点需要输出的 Payload 类型
     *
     * @return 依赖 Payload 类型的Class对象
     */
    public Class<?> getRequiredType() {
        return requiredType;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DependencyDescriptor that = (DependencyDescriptor) o;
        return name.equals(that.name) && requiredType.equals(that.requiredType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, requiredType);
    }

    @Override
    public String toString() {
        // 明确指出是 Payload 类型
        return String.format("依赖[%s:Payload(%s)]", name, requiredType.getSimpleName());
    }
}
