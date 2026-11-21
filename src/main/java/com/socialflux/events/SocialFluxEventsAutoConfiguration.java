package com.socialflux.events;

import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.QueueBuilder;
import org.springframework.amqp.core.TopicExchange;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.transaction.PlatformTransactionManager;
import tools.jackson.databind.json.JsonMapper;

import java.time.Clock;

/**
 * Registra a infraestrutura de eventos em qualquer serviço que tenha a biblioteca no classpath:
 * <ul>
 *   <li>sempre: {@link Outbox}, {@link OutboxRelay} e {@link Inbox} (gravar e consumir funcionam sem broker);</li>
 *   <li>com {@code socialflux.events.enabled=true}: {@link AmqpEventPublisher} e a topologia compartilhada
 *   (exchange {@code topic} de eventos, exchange e fila de mensagens mortas).</li>
 * </ul>
 * Cada serviço continua dono das suas tabelas ({@code outbox_event}, {@code processed_message}), filas e handlers.
 */
@AutoConfiguration
@EnableConfigurationProperties(SocialFluxEventsProperties.class)
public class SocialFluxEventsAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    Clock socialfluxEventsClock() {
        return Clock.systemUTC();
    }

    @Bean
    @ConditionalOnMissingBean
    Outbox outbox(JdbcClient jdbc, JsonMapper mapper, Clock clock) {
        return new Outbox(jdbc, mapper, clock);
    }

    @Bean
    @ConditionalOnMissingBean
    OutboxRelay outboxRelay(Outbox outbox, ObjectProvider<EventPublisher> publisher, SocialFluxEventsProperties properties,
                            Clock clock, PlatformTransactionManager txManager) {
        return new OutboxRelay(outbox, publisher, properties, clock, txManager);
    }

    @Bean
    @ConditionalOnMissingBean
    Inbox inbox(JdbcClient jdbc, Clock clock) {
        return new Inbox(jdbc, clock);
    }

    /** Broker: só existe com {@code socialflux.events.enabled=true}. */
    @Configuration(proxyBeanMethods = false)
    @ConditionalOnProperty(prefix = "socialflux.events", name = "enabled", havingValue = "true")
    static class Amqp {

        @Bean
        @ConditionalOnMissingBean(EventPublisher.class)
        AmqpEventPublisher amqpEventPublisher(RabbitTemplate template, SocialFluxEventsProperties properties) {
            return new AmqpEventPublisher(template, properties);
        }

        @Bean(EventQueues.EXCHANGE_BEAN)
        TopicExchange socialfluxEventsExchange(SocialFluxEventsProperties properties) {
            return new TopicExchange(properties.exchange(), true, false);
        }

        @Bean
        TopicExchange socialfluxDeadLetterExchange() {
            return new TopicExchange(EventQueues.DEAD_EXCHANGE, true, false);
        }

        @Bean
        Queue socialfluxDeadLetterQueue() {
            return QueueBuilder.durable(EventQueues.DEAD_QUEUE).build();
        }

        @Bean
        Binding socialfluxDeadLetterBinding(@Qualifier("socialfluxDeadLetterQueue") Queue deadQueue,
                                            @Qualifier("socialfluxDeadLetterExchange") TopicExchange deadExchange) {
            return BindingBuilder.bind(deadQueue).to(deadExchange).with("#");
        }
    }
}
