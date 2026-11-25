package com.socialflux.events;

import com.socialflux.events.logging.CorrelationId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.transaction.annotation.Transactional;

import java.time.Clock;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.UUID;

/**
 * Idempotência de consumidor: cada mensagem (por id) é processada no máximo uma vez, na mesma transação do efeito.
 * <p>Requer a tabela {@code processed_message} na migração do serviço (ver README da biblioteca).
 */
public class Inbox {

    private static final Logger log = LoggerFactory.getLogger(Inbox.class);

    private final JdbcClient jdbc;
    private final Clock clock;

    public Inbox(JdbcClient jdbc, Clock clock) {
        this.jdbc = jdbc;
        this.clock = clock;
    }

    /** Executa {@code action} só se a mensagem ainda não foi processada. Devolve se executou. */
    @Transactional
    public boolean once(UUID messageId, Runnable action) {
        int inserted = jdbc.sql("insert into processed_message (id, processed_at) values (:id, :now) "
                        + "on conflict (id) do nothing")
                .param("id", messageId)
                .param("now", OffsetDateTime.ofInstant(clock.instant(), ZoneOffset.UTC))
                .update();
        if (inserted == 0) {
            return false;
        }
        action.run();
        return true;
    }

    /**
     * Como {@link #once(UUID, Runnable)}, mas continua o fluxo do evento: o id de correlação do envelope vai para o MDC
     * (todo log do efeito o carrega) e o consumo é registrado (consumido, duplicado ou falha). Prefira este nos handlers.
     */
    @Transactional
    public boolean once(EventEnvelope event, Runnable action) {
        try (CorrelationId.Scope ignored = CorrelationId.open(event.correlationId())) {
            boolean executed = once(event.id(), action);
            if (executed) {
                log.info("Evento consumido: {} (id {}, agregado {}, v{})", event.type(), event.id(),
                        event.aggregateId(), event.aggregateVersion());
            } else {
                log.info("Evento duplicado ignorado: {} (id {})", event.type(), event.id());
            }
            return executed;
        } catch (RuntimeException e) {
            log.warn("Falha ao processar o evento {} (id {}): {}", event.type(), event.id(), e.toString());
            throw e;
        }
    }
}
