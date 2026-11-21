package com.socialflux.events;

import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.QueueBuilder;
import org.springframework.amqp.core.TopicExchange;

/**
 * Nomes da topologia compartilhada e atalhos para um serviço declarar a SUA fila de consumo.
 * <pre>{@code
 * @Bean Queue minhaFila() { return EventQueues.durable("meu-servico.profile-events"); }
 * @Bean Binding minhaLigacao(Queue minhaFila, @Qualifier(EventQueues.EXCHANGE_BEAN) TopicExchange exchange) {
 *     return EventQueues.bind(minhaFila, exchange, "profile.#");
 * }
 * }</pre>
 */
public final class EventQueues {

    /** Nome do bean do exchange de eventos (para {@code @Qualifier}). */
    public static final String EXCHANGE_BEAN = "socialfluxEventsExchange";
    public static final String DEAD_EXCHANGE = "socialflux.events.dlx";
    public static final String DEAD_QUEUE = "socialflux.events.dead";

    private EventQueues() {
    }

    /** Fila durável cujas mensagens rejeitadas (após as tentativas) vão para a fila de mensagens mortas. */
    public static Queue durable(String name) {
        return QueueBuilder.durable(name).deadLetterExchange(DEAD_EXCHANGE).build();
    }

    /** Liga a fila ao exchange de eventos por um padrão de routing key (ex.: {@code profile.#}). */
    public static Binding bind(Queue queue, TopicExchange exchange, String pattern) {
        return BindingBuilder.bind(queue).to(exchange).with(pattern);
    }
}
