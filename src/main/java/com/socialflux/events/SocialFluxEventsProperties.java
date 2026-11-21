package com.socialflux.events;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

import java.time.Duration;

/**
 * Configuração da infraestrutura de eventos ({@code socialflux.events.*}). Cada serviço aponta essas chaves para as suas
 * variáveis de ambiente no {@code application.yml}.
 *
 * @param enabled       liga o RabbitMQ (publicador, topologia, relay agendado). Desligado, o outbox continua gravando
 * @param exchange      exchange {@code topic} compartilhado (routing key = tipo do evento)
 * @param mandatory     evento sem nenhuma fila ligada é tratado como FALHA e fica pendente no outbox (use quando o evento
 *                      tem um consumidor obrigatório que pode ainda não ter subido)
 * @param relayInterval intervalo entre as publicações do outbox
 * @param batchSize     eventos publicados por rodada do relay
 * @param retention     por quanto tempo eventos já publicados ficam no outbox
 */
@ConfigurationProperties(prefix = "socialflux.events")
public record SocialFluxEventsProperties(
        @DefaultValue("false") boolean enabled,
        @DefaultValue("socialflux.events") String exchange,
        @DefaultValue("false") boolean mandatory,
        @DefaultValue("PT0.5S") Duration relayInterval,
        @DefaultValue("100") int batchSize,
        @DefaultValue("P7D") Duration retention) {

    public SocialFluxEventsProperties {
        if (exchange == null || exchange.isBlank()) {
            throw new IllegalArgumentException("socialflux.events.exchange não pode ser vazio");
        }
        if (batchSize < 1) {
            throw new IllegalArgumentException("socialflux.events.batch-size deve ser >= 1");
        }
    }
}
