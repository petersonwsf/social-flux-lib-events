package com.socialflux.events;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Clock;

/**
 * Publica os eventos pendentes do outbox (pelo menos uma vez, em ordem). Falha de broker não perde nada: tenta de novo.
 * O agendamento só roda se o serviço tiver {@code @EnableScheduling} e {@code socialflux.events.enabled=true};
 * {@link #publishPending()} pode ser chamado diretamente (ex.: testes com um {@link EventPublisher} falso).
 */
public class OutboxRelay {

    private static final Logger log = LoggerFactory.getLogger(OutboxRelay.class);

    private final Outbox outbox;
    private final ObjectProvider<EventPublisher> publisher;
    private final SocialFluxEventsProperties properties;
    private final Clock clock;
    private final TransactionTemplate tx;

    public OutboxRelay(Outbox outbox, ObjectProvider<EventPublisher> publisher, SocialFluxEventsProperties properties,
                       Clock clock, PlatformTransactionManager txManager) {
        this.outbox = outbox;
        this.publisher = publisher;
        this.properties = properties;
        this.clock = clock;
        this.tx = new TransactionTemplate(txManager);
    }

    @Scheduled(fixedDelayString = "${socialflux.events.relay-interval:PT0.5S}")
    void scheduled() {
        if (!properties.enabled()) {
            return;
        }
        try {
            publishPending();
        } catch (RuntimeException e) {
            log.warn("Falha ao publicar eventos do outbox (tentará de novo): {}", e.toString());
        }
    }

    /** Publica um lote e devolve quantos eventos foram enviados. */
    public int publishPending() {
        EventPublisher target = publisher.getIfAvailable();
        if (target == null) {
            return 0;
        }
        Integer sent = tx.execute(status -> {
            int count = 0;
            for (Outbox.Row row : outbox.lockPending(properties.batchSize())) {
                try {
                    target.publish(row.id(), row.type(), row.payload());
                } catch (RuntimeException e) {
                    log.warn("Não foi possível publicar {} (fica pendente): {}", row.type(), e.toString());
                    break; // preserva a ordem: não pula o evento que falhou
                }
                outbox.markPublished(row.id());
                count++;
            }
            return count;
        });
        return sent == null ? 0 : sent;
    }

    /** Remove eventos já publicados há mais tempo que {@code socialflux.events.retention}. */
    @Scheduled(fixedDelayString = "PT6H", initialDelayString = "PT1M")
    void purge() {
        try {
            int removed = outbox.deletePublishedBefore(clock.instant().minus(properties.retention()));
            if (removed > 0) {
                log.info("Outbox: {} eventos antigos removidos", removed);
            }
        } catch (RuntimeException e) {
            log.warn("Falha ao limpar o outbox: {}", e.toString());
        }
    }
}
