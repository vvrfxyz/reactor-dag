// src/main/java/xyz/vvrf/reactor/dag/monitor/web/autoconfigure/DagMonitorWebAutoConfiguration.java
package xyz.vvrf.reactor.dag.monitor.web.autoconfigure;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnWebApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import xyz.vvrf.reactor.dag.monitor.web.controller.DagMonitoringController;
import xyz.vvrf.reactor.dag.monitor.web.service.DagDefinitionCache;
import xyz.vvrf.reactor.dag.monitor.web.service.RealtimeDagMonitorListener;
import xyz.vvrf.reactor.dag.spring.boot.DagFrameworkAutoConfiguration;

@Configuration
@ConditionalOnWebApplication(type = ConditionalOnWebApplication.Type.REACTIVE)
@AutoConfigureAfter(DagFrameworkAutoConfiguration.class)
@ComponentScan(basePackages = "xyz.vvrf.reactor.dag.monitor.web.service")
public class DagMonitorWebAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    public DagDefinitionCache dagDefinitionCache() {
        return new DagDefinitionCache();
    }

    @Bean
    @ConditionalOnMissingBean
    public DagMonitoringController dagMonitoringController(
            RealtimeDagMonitorListener monitorListenerService,
            DagDefinitionCache dagDefinitionCache,
            ObjectMapper objectMapper) {
        return new DagMonitoringController(monitorListenerService, dagDefinitionCache, objectMapper);
    }
}
