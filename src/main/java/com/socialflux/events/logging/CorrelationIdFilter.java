package com.socialflux.events.logging;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;

/**
 * Primeiro filtro de cada requisição: adota o {@code X-Correlation-Id} que o gateway (ou outro serviço) mandou, ou cria um,
 * coloca no MDC (todas as linhas de log da requisição passam a carregá-lo), devolve no cabeçalho da resposta e registra uma
 * linha de acesso ao final. Roda antes da segurança, então até um 401/403 sai com o id.
 */
public class CorrelationIdFilter extends OncePerRequestFilter {

    private static final Logger log = LoggerFactory.getLogger("com.socialflux.http.access");

    private final SocialFluxLoggingProperties properties;

    public CorrelationIdFilter(SocialFluxLoggingProperties properties) {
        this.properties = properties;
    }

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain chain)
            throws ServletException, IOException {
        try (CorrelationId.Scope ignored = CorrelationId.open(request.getHeader(CorrelationId.HEADER))) {
            response.setHeader(CorrelationId.HEADER, CorrelationId.current());
            long start = System.nanoTime();
            int status = HttpServletResponse.SC_INTERNAL_SERVER_ERROR;   // exceção que escapa = 500
            try {
                chain.doFilter(request, response);
                status = response.getStatus();
            } finally {
                logAccess(request, status, (System.nanoTime() - start) / 1_000_000);
            }
        }
    }

    private void logAccess(HttpServletRequest request, int status, long millis) {
        if (!properties.accessLog()) {
            return;
        }
        String path = request.getRequestURI();
        Level level = isQuiet(path) ? Level.DEBUG : status >= 500 ? Level.WARN : Level.INFO;
        if (!log.isEnabledForLevel(level)) {
            return;
        }
        log.atLevel(level)
                .addKeyValue("method", request.getMethod())
                .addKeyValue("path", path)
                .addKeyValue("status", status)
                .addKeyValue("durationMs", millis)
                .log("Requisição {} {} respondeu {} em {} ms", request.getMethod(), path, status, millis);
    }

    private boolean isQuiet(String path) {
        return properties.quietPaths().stream().anyMatch(path::startsWith);
    }
}
