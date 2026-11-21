package com.socialflux.events;

import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageDeliveryMode;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.core.RabbitTemplate;

import java.nio.charset.StandardCharsets;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Publica no RabbitMQ com mensagem persistente e CONFIRMAÇÃO do broker (publisher confirms). Com {@code mandatory}, um
 * evento sem nenhuma fila ligada é tratado como FALHA e continua pendente no outbox (exige
 * {@code spring.rabbitmq.publisher-returns: true}).
 */
public class AmqpEventPublisher implements EventPublisher {

    private static final long CONFIRM_TIMEOUT_MS = 5_000;

    private final RabbitTemplate template;
    private final String exchange;
    private final boolean mandatory;
    private final Set<String> unroutable = ConcurrentHashMap.newKeySet();

    public AmqpEventPublisher(RabbitTemplate template, SocialFluxEventsProperties properties) {
        this.template = template;
        this.exchange = properties.exchange();
        this.mandatory = properties.mandatory();
        if (mandatory) {
            template.setMandatory(true);
            template.setReturnsCallback(returned -> {
                String id = returned.getMessage().getMessageProperties().getMessageId();
                if (id != null) {
                    unroutable.add(id);
                }
            });
        }
    }

    @Override
    public void publish(UUID messageId, String routingKey, String json) {
        MessageProperties properties = new MessageProperties();
        properties.setContentType(MessageProperties.CONTENT_TYPE_JSON);
        properties.setContentEncoding(StandardCharsets.UTF_8.name());
        properties.setMessageId(messageId.toString());
        properties.setDeliveryMode(MessageDeliveryMode.PERSISTENT);
        Message message = new Message(json.getBytes(StandardCharsets.UTF_8), properties);
        unroutable.remove(messageId.toString());
        template.invoke(operations -> {
            operations.send(exchange, routingKey, message);
            operations.waitForConfirmsOrDie(CONFIRM_TIMEOUT_MS);
            return true;
        });
        // O broker devolve (basic.return) ANTES de confirmar; se voltou, ninguém está escutando este evento ainda.
        if (mandatory && unroutable.remove(messageId.toString())) {
            throw new IllegalStateException("Nenhuma fila ligada a " + routingKey + " (o consumidor ainda não subiu?)");
        }
    }
}
