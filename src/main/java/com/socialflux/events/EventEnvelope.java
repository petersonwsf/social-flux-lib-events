package com.socialflux.events;

import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import java.util.UUID;

/**
 * Envelope de evento lido por um consumidor: {@code {id, type, occurredAt, aggregateId, aggregateVersion, data}}.
 * {@code data} fica como árvore JSON: cada consumidor lê só os campos que conhece (mudanças aditivas não quebram).
 */
public record EventEnvelope(UUID id, String type, String occurredAt, UUID aggregateId, long aggregateVersion,
                            JsonNode data) {

    public static EventEnvelope parse(JsonMapper mapper, String json) {
        JsonNode event = mapper.readTree(json);
        return new EventEnvelope(
                UUID.fromString(required(event, "id")),
                required(event, "type"),
                event.path("occurredAt").asString(null),
                UUID.fromString(required(event, "aggregateId")),
                event.path("aggregateVersion").asLong(),
                event.path("data"));
    }

    private static String required(JsonNode event, String field) {
        JsonNode node = event.path(field);
        if (node.isMissingNode() || node.isNull() || node.asString().isBlank()) {
            throw new IllegalArgumentException("Evento sem o campo obrigatório '" + field + "'");
        }
        return node.asString();
    }
}
