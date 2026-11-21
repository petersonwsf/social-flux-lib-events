package com.socialflux.events;

import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.TopicExchange;
import org.springframework.boot.amqp.autoconfigure.RabbitAutoConfiguration;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.IllegalTransactionStateException;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.annotation.EnableTransactionManagement;
import org.springframework.transaction.support.TransactionTemplate;
import tools.jackson.databind.json.JsonMapper;

import javax.sql.DataSource;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Outbox, relay, inbox e auto-configuração contra um PostgreSQL real (embutido), como nos serviços. */
class SocialFluxEventsTest {

    private static EmbeddedPostgres postgres;

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(SocialFluxEventsAutoConfiguration.class))
            .withUserConfiguration(Infra.class);

    @BeforeAll
    static void start() throws IOException {
        postgres = EmbeddedPostgres.builder().start();
        JdbcClient jdbc = JdbcClient.create(postgres.getPostgresDatabase());
        // Mesmo esquema que os serviços criam nas próprias migrações.
        jdbc.sql("""
                create table outbox_event (
                    seq bigint generated always as identity primary key,
                    id uuid not null unique,
                    type varchar(80) not null,
                    aggregate_id uuid not null,
                    aggregate_version bigint not null,
                    payload text not null,
                    created_at timestamp with time zone not null,
                    published_at timestamp with time zone)""").update();
        jdbc.sql("create table processed_message (id uuid primary key, processed_at timestamp with time zone not null)")
                .update();
    }

    @AfterAll
    static void stop() throws IOException {
        postgres.close();
    }

    @BeforeEach
    void clean() {
        JdbcClient jdbc = JdbcClient.create(postgres.getPostgresDatabase());
        jdbc.sql("delete from outbox_event").update();
        jdbc.sql("delete from processed_message").update();
    }

    @Test
    void outboxRequiresAnActiveTransaction() {
        runner.run(ctx -> assertThatThrownBy(() -> ctx.getBean(Outbox.class)
                .add("teste.criado.v1", UUID.randomUUID(), 1, Map.of()))
                .isInstanceOf(IllegalTransactionStateException.class));
    }

    @Test
    void relayPublishesInOrderWithTheEnvelopeAndMarksAsPublished() {
        runner.run(ctx -> {
            Outbox outbox = ctx.getBean(Outbox.class);
            UUID aggregate = UUID.randomUUID();
            inTransaction(ctx.getBean(PlatformTransactionManager.class), () -> {
                outbox.add("teste.criado.v1", aggregate, 1, Map.of("nome", "a"));
                outbox.add("teste.alterado.v1", aggregate, 2, Map.of("nome", "b"));
            });

            int sent = ctx.getBean(OutboxRelay.class).publishPending();

            Recording publisher = ctx.getBean(Recording.class);
            assertThat(sent).isEqualTo(2);
            assertThat(publisher.sent).extracting(Recording.Sent::routingKey)
                    .containsExactly("teste.criado.v1", "teste.alterado.v1");
            EventEnvelope first = EventEnvelope.parse(JsonMapper.builder().build(), publisher.sent.get(0).json());
            assertThat(first.id()).isEqualTo(publisher.sent.get(0).id());
            assertThat(first.aggregateId()).isEqualTo(aggregate);
            assertThat(first.aggregateVersion()).isEqualTo(1);
            assertThat(first.occurredAt()).isNotBlank();
            assertThat(first.data().path("nome").asString()).isEqualTo("a");
            assertThat(outbox.countPending()).isZero();
            assertThat(ctx.getBean(OutboxRelay.class).publishPending()).isZero();
        });
    }

    @Test
    void failedPublicationStaysPendingAndKeepsTheOrder() {
        runner.run(ctx -> {
            Outbox outbox = ctx.getBean(Outbox.class);
            Recording publisher = ctx.getBean(Recording.class);
            inTransaction(ctx.getBean(PlatformTransactionManager.class), () -> {
                outbox.add("teste.um.v1", UUID.randomUUID(), 1, Map.of());
                outbox.add("teste.dois.v1", UUID.randomUUID(), 1, Map.of());
            });

            publisher.failOnCall = 2;   // o segundo evento falha
            assertThat(ctx.getBean(OutboxRelay.class).publishPending()).isEqualTo(1);
            assertThat(outbox.countPending()).isEqualTo(1);

            publisher.failOnCall = 0;
            assertThat(ctx.getBean(OutboxRelay.class).publishPending()).isEqualTo(1);
            assertThat(publisher.sent).extracting(Recording.Sent::routingKey)
                    .containsExactly("teste.um.v1", "teste.dois.v1");
        });
    }

    @Test
    void inboxRunsEachMessageAtMostOnce() {
        runner.run(ctx -> {
            Inbox inbox = ctx.getBean(Inbox.class);
            UUID message = UUID.randomUUID();
            AtomicInteger runs = new AtomicInteger();

            assertThat(inbox.once(message, runs::incrementAndGet)).isTrue();
            assertThat(inbox.once(message, runs::incrementAndGet)).isFalse();
            assertThat(runs).hasValue(1);
        });
    }

    @Test
    void inboxDoesNotRecordTheMessageWhenTheActionFails() {
        runner.run(ctx -> {
            Inbox inbox = ctx.getBean(Inbox.class);
            UUID message = UUID.randomUUID();

            assertThatThrownBy(() -> inbox.once(message, () -> {
                throw new IllegalStateException("falha no efeito");
            })).isInstanceOf(IllegalStateException.class);

            assertThat(inbox.once(message, () -> { })).isTrue();   // a nova entrega é processada
        });
    }

    @Test
    void envelopeRejectsMissingRequiredFields() {
        JsonMapper mapper = JsonMapper.builder().build();
        assertThatThrownBy(() -> EventEnvelope.parse(mapper, "{\"type\":\"x.v1\",\"aggregateId\":\"" + UUID.randomUUID() + "\"}"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("id");
    }

    @Test
    void brokerBeansOnlyExistWhenEnabled() {
        runner.run(ctx -> {
            assertThat(ctx).doesNotHaveBean(AmqpEventPublisher.class);
            assertThat(ctx).doesNotHaveBean(TopicExchange.class);
        });

        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(RabbitAutoConfiguration.class,
                        SocialFluxEventsAutoConfiguration.class))
                .withUserConfiguration(InfraWithoutPublisher.class)
                .withPropertyValues("socialflux.events.enabled=true", "socialflux.events.exchange=teste.events",
                        "socialflux.events.mandatory=true")
                .run(ctx -> {
                    assertThat(ctx).hasSingleBean(AmqpEventPublisher.class);
                    assertThat(ctx.getBean(EventQueues.EXCHANGE_BEAN, TopicExchange.class).getName())
                            .isEqualTo("teste.events");
                    assertThat(ctx.getBean("socialfluxDeadLetterExchange", TopicExchange.class).getName())
                            .isEqualTo(EventQueues.DEAD_EXCHANGE);
                    assertThat(ctx.getBean(SocialFluxEventsProperties.class).mandatory()).isTrue();
                });
    }

    @Test
    void invalidConfigurationFailsFast() {
        runner.withPropertyValues("socialflux.events.batch-size=0")
                .run(ctx -> assertThat(ctx).hasFailed());
    }

    private static void inTransaction(PlatformTransactionManager txManager, Runnable action) {
        new TransactionTemplate(txManager).executeWithoutResult(status -> action.run());
    }

    /** Infra mínima que os serviços já têm via Spring Boot: banco, JdbcClient, JsonMapper e transações. */
    @Configuration(proxyBeanMethods = false)
    @EnableTransactionManagement
    static class InfraWithoutPublisher {

        @Bean
        DataSource dataSource() {
            return postgres.getPostgresDatabase();
        }

        @Bean
        JdbcClient jdbcClient(DataSource dataSource) {
            return JdbcClient.create(dataSource);
        }

        @Bean
        JsonMapper jsonMapper() {
            return JsonMapper.builder().build();
        }

        @Bean
        PlatformTransactionManager transactionManager(DataSource dataSource) {
            return new DataSourceTransactionManager(dataSource);
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class Infra extends InfraWithoutPublisher {

        @Bean
        Recording recordingPublisher() {
            return new Recording();
        }
    }

    /** Substitui o RabbitMQ: guarda o que foi publicado e simula falha numa chamada específica. */
    static class Recording implements EventPublisher {

        record Sent(UUID id, String routingKey, String json) {
        }

        final List<Sent> sent = new CopyOnWriteArrayList<>();
        private final AtomicInteger calls = new AtomicInteger();
        volatile int failOnCall;

        @Override
        public void publish(UUID messageId, String routingKey, String json) {
            if (calls.incrementAndGet() == failOnCall) {
                throw new IllegalStateException("broker fora do ar (simulado)");
            }
            sent.add(new Sent(messageId, routingKey, json));
        }
    }
}
