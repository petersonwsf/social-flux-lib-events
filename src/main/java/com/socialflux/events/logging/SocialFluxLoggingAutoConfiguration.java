package com.socialflux.events.logging;

import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnWebApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.web.servlet.FilterRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.core.Ordered;
import org.springframework.web.filter.OncePerRequestFilter;

/**
 * Registra o {@link CorrelationIdFilter} (primeiro da cadeia, antes do Spring Security) em aplicações web servlet. Fora de
 * contexto web (testes de biblioteca, jobs) não faz nada; o {@link CorrelationId} continua utilizável direto.
 */
@AutoConfiguration
@ConditionalOnWebApplication(type = ConditionalOnWebApplication.Type.SERVLET)
@ConditionalOnClass(OncePerRequestFilter.class)
@EnableConfigurationProperties(SocialFluxLoggingProperties.class)
public class SocialFluxLoggingAutoConfiguration {

    @Bean
    FilterRegistrationBean<CorrelationIdFilter> socialfluxCorrelationIdFilter(SocialFluxLoggingProperties properties) {
        FilterRegistrationBean<CorrelationIdFilter> registration =
                new FilterRegistrationBean<>(new CorrelationIdFilter(properties));
        registration.setOrder(Ordered.HIGHEST_PRECEDENCE);
        return registration;
    }
}
