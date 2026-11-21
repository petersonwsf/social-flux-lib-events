package com.socialflux.events;

import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import tools.jackson.databind.json.JsonMapper;

import java.time.Clock;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Transactional outbox: {@link #add} grava o evento na MESMA transação da mudança de dados (por isso exige uma transação
 * ativa); o {@link OutboxRelay} publica depois. Isso evita "gravei no banco mas perdi o evento" e vice-versa.
 * <p>Requer a tabela {@code outbox_event} na migração do serviço (ver README da biblioteca).
 */
public class Outbox {

    public record Row(UUID id, String type, String payload) {
    }

    private final JdbcClient jdbc;
    private final JsonMapper mapper;
    private final Clock clock;

    public Outbox(JdbcClient jdbc, JsonMapper mapper, Clock clock) {
        this.jdbc = jdbc;
        this.mapper = mapper;
        this.clock = clock;
    }

    /**
     * Envelope: {id, type, occurredAt, aggregateId, aggregateVersion, data}. Só mudanças aditivas por versão de tipo;
     * {@code data} nunca carrega dados pessoais (e-mail, senha, telefone, sexo, nascimento).
     */
    @Transactional(propagation = Propagation.MANDATORY)
    public void add(String type, UUID aggregateId, long aggregateVersion, Map<String, Object> data) {
        UUID id = EventIds.v7(clock);
        Instant now = clock.instant();
        Map<String, Object> envelope = new LinkedHashMap<>();
        envelope.put("id", id.toString());
        envelope.put("type", type);
        envelope.put("occurredAt", now.toString());
        envelope.put("aggregateId", aggregateId.toString());
        envelope.put("aggregateVersion", aggregateVersion);
        envelope.put("data", data);
        jdbc.sql("insert into outbox_event (id, type, aggregate_id, aggregate_version, payload, created_at) "
                        + "values (:id, :type, :aggregate, :version, :payload, :createdAt)")
                .param("id", id)
                .param("type", type)
                .param("aggregate", aggregateId)
                .param("version", aggregateVersion)
                .param("payload", mapper.writeValueAsString(envelope))
                .param("createdAt", OffsetDateTime.ofInstant(now, ZoneOffset.UTC))
                .update();
    }

    /** Eventos ainda não publicados, em ordem, travados contra outras instâncias do relay. */
    @Transactional(propagation = Propagation.MANDATORY)
    public List<Row> lockPending(int limit) {
        return jdbc.sql("select id, type, payload from outbox_event where published_at is null "
                        + "order by seq limit :limit for update skip locked")
                .param("limit", limit)
                .query((rs, n) -> new Row(rs.getObject("id", UUID.class), rs.getString("type"), rs.getString("payload")))
                .list();
    }

    @Transactional(propagation = Propagation.MANDATORY)
    public void markPublished(UUID id) {
        jdbc.sql("update outbox_event set published_at = :now where id = :id")
                .param("now", OffsetDateTime.ofInstant(clock.instant(), ZoneOffset.UTC))
                .param("id", id)
                .update();
    }

    public int deletePublishedBefore(Instant limit) {
        return jdbc.sql("delete from outbox_event where published_at is not null and published_at < :limit")
                .param("limit", OffsetDateTime.ofInstant(limit, ZoneOffset.UTC))
                .update();
    }

    public long countPending() {
        return jdbc.sql("select count(*) from outbox_event where published_at is null").query(Long.class).single();
    }
}
