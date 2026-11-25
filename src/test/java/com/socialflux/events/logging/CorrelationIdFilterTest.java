package com.socialflux.events.logging;

import jakarta.servlet.ServletException;
import org.junit.jupiter.api.Test;
import org.slf4j.MDC;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CorrelationIdFilterTest {

    private final CorrelationIdFilter filter =
            new CorrelationIdFilter(new SocialFluxLoggingProperties(true, List.of("/actuator")));

    @Test
    void adoptsTheIncomingIdPutsItInTheMdcAndEchoesItBack() throws ServletException, IOException {
        MockHttpServletRequest request = new MockHttpServletRequest("GET", "/api/postagens");
        request.addHeader(CorrelationId.HEADER, "abc-12345678");
        MockHttpServletResponse response = new MockHttpServletResponse();
        AtomicReference<String> insideTheChain = new AtomicReference<>();

        filter.doFilter(request, response, (req, res) -> insideTheChain.set(MDC.get(CorrelationId.MDC_KEY)));

        assertThat(insideTheChain.get()).isEqualTo("abc-12345678");
        assertThat(response.getHeader(CorrelationId.HEADER)).isEqualTo("abc-12345678");
        assertThat(MDC.get(CorrelationId.MDC_KEY)).isNull();   // limpou ao terminar (a thread volta para o pool)
    }

    @Test
    void generatesAnIdWhenMissingOrWhenTheIncomingOneIsUnsafe() throws ServletException, IOException {
        for (String incoming : new String[]{null, "curto", "com espaço e\r\nquebra de linha", "x".repeat(65)}) {
            MockHttpServletRequest request = new MockHttpServletRequest("GET", "/api/postagens");
            if (incoming != null) {
                request.addHeader(CorrelationId.HEADER, incoming);
            }
            MockHttpServletResponse response = new MockHttpServletResponse();

            filter.doFilter(request, response, (req, res) -> { });

            String id = response.getHeader(CorrelationId.HEADER);
            assertThat(id).isNotEqualTo(incoming);
            assertThat(CorrelationId.isValid(id)).isTrue();
        }
    }

    @Test
    void clearsTheMdcEvenWhenTheChainFails() {
        MockHttpServletRequest request = new MockHttpServletRequest("GET", "/api/postagens");

        assertThatThrownBy(() -> filter.doFilter(request, new MockHttpServletResponse(), (req, res) -> {
            throw new IllegalStateException("erro inesperado");
        })).isInstanceOf(IllegalStateException.class);

        assertThat(MDC.get(CorrelationId.MDC_KEY)).isNull();
    }

    @Test
    void scopeRestoresThePreviousId() {
        try (CorrelationId.Scope outer = CorrelationId.open("externo-12345")) {
            try (CorrelationId.Scope inner = CorrelationId.open("interno-12345")) {
                assertThat(CorrelationId.current()).isEqualTo("interno-12345");
            }
            assertThat(CorrelationId.current()).isEqualTo("externo-12345");
        }
        assertThat(CorrelationId.current()).isNull();
    }
}
