package com.socialflux.events.logging;

import org.springframework.http.HttpRequest;
import org.springframework.http.client.ClientHttpRequestExecution;
import org.springframework.http.client.ClientHttpRequestInterceptor;
import org.springframework.http.client.ClientHttpResponse;

import java.io.IOException;

/**
 * Repassa o id de correlação do fluxo atual nas chamadas HTTP ENTRE SERVIÇOS (RestClient/RestTemplate). Não use em chamadas a
 * terceiros (ex.: API de moderação): o id é interno.
 */
public class CorrelationIdClientInterceptor implements ClientHttpRequestInterceptor {

    @Override
    public ClientHttpResponse intercept(HttpRequest request, byte[] body, ClientHttpRequestExecution execution)
            throws IOException {
        String id = CorrelationId.current();
        if (id != null) {
            request.getHeaders().set(CorrelationId.HEADER, id);
        }
        return execution.execute(request, body);
    }
}
