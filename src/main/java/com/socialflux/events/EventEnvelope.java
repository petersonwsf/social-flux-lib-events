package com.socialflux.events;

import com.socialflux.events.logging.CorrelationId;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import java.util.UUID;

/**
 * Envelope de evento lido por um consumidor: {@code {id, type, occurredAt, aggregateId, aggregateVersion, data}}.
 * {@code data} fica como árvore JSON: cada consumidor lê só os campos que conhece (mudanças aditivas não quebram).
 * {@code correlationId} (opcional) é o id de correlação do fluxo que gerou o evento; {@code null} em eventos antigos.
 */
public record EventEnvelope(UUID id, String type, String occurredAt, UUID aggregateId, long aggregateVersion,
                            JsonNode data, String correlationId) {

    public static EventEnvelope parse(JsonMapper mapper, String json) {
        JsonNode event = mapper.readTree(json);
        return new EventEnvelope(
                UUID.fromString(required(event, "id")),
                required(event, "type"),
                event.path("occurredAt").asString(null),
                UUID.fromString(required(event, "aggregateId")),
                event.path("aggregateVersion").asLong(),
                event.path("data"),
                correlationId(event));
    }

    /** Só aceita ids no formato esperado (o valor vem de uma mensagem e vai parar nos logs). */
    private static String correlationId(JsonNode event) {
        String value = event.path("correlationId").asString(null);
        return CorrelationId.isValid(value) ? value : null;
    }

    private static String required(JsonNode event, String field) {
        JsonNode node = event.path(field);
        if (node.isMissingNode() || node.isNull() || node.asString().isBlank()) {
            throw new IllegalArgumentException("Evento sem o campo obrigatório '" + field + "'");
        }
        return node.asString();
    }
}
