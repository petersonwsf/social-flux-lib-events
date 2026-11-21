package com.socialflux.events;

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
}
