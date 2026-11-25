package com.socialflux.events.logging;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

import java.util.List;

/**
 * Propriedades {@code socialflux.logging.*}.
 *
 * @param accessLog  registra uma linha por requisição HTTP (método, caminho, status, duração; nunca a query string)
 * @param quietPaths prefixos de caminho cujo log de acesso vai para DEBUG (health checks e a verificação de sessão que o
 *                   gateway faz a cada requisição), para não afogar o Loki
 */
@ConfigurationProperties(prefix = "socialflux.logging")
public record SocialFluxLoggingProperties(
        @DefaultValue("true") boolean accessLog,
        @DefaultValue({"/actuator", "/internal/auth/verify"}) List<String> quietPaths) {
}
