# socialflux-events

Biblioteca compartilhada com a **infraestrutura de eventos** dos serviços Spring Boot do Social Flux: outbox transacional,
relay para o RabbitMQ, inbox idempotente e a topologia comum. Usada por `authentication_service` e `profile_service` (e pelos
próximos serviços). Spring Boot 4.1 · Java 21 · auto-configuração (basta estar no classpath).

Decisão: D13 de [`../../docs/plano-dominio-relacoes-grafo-social.md`](../../docs/plano-dominio-relacoes-grafo-social.md).

## O que ela entrega

| Bean / classe | Quando existe | Para que serve |
| --- | --- | --- |
| `Outbox` | sempre | `add(type, aggregateId, version, data)` grava o evento em `outbox_event` **na transação atual** (exige transação: `MANDATORY`) |
| `OutboxRelay` | sempre | publica os pendentes em ordem (agendado, se o serviço tem `@EnableScheduling` e `enabled=true`); `publishPending()` pode ser chamado nos testes; expurga os publicados após `retention` |
| `Inbox` | sempre | `once(messageId, action)`: efeito executado no máximo uma vez por mensagem (tabela `processed_message`) |
| `EventPublisher` (interface) | — | porta de saída; nos testes, troque por um bean `@Primary` que grava em memória |
| `AmqpEventPublisher` | `enabled=true` e sem outro `EventPublisher` | mensagem persistente, *publisher confirms*, `mandatory` opcional |
| Exchange `socialflux.events` (bean `socialfluxEventsExchange`), DLX `socialflux.events.dlx`, fila `socialflux.events.dead` | `enabled=true` | topologia compartilhada |
| `EventEnvelope` | — | leitura do envelope `{id, type, occurredAt, aggregateId, aggregateVersion, data}` pelos consumidores |
| `EventQueues` | — | `durable(nome)` (fila com DLX) e `bind(fila, exchange, padrão)` para o serviço declarar a **sua** fila |
| `Clock` | só se o serviço não declarar um | relógio UTC padrão |

O que **não** está aqui: migrações (cada serviço cria as tabelas na sua própria migração Flyway), tipos de evento, handlers e
filas de cada serviço.

## Como usar num serviço

1. Dependência no `pom.xml` do serviço:
   ```xml
   <dependency>
       <groupId>com.socialflux</groupId>
       <artifactId>socialflux-events</artifactId>
       <version>0.1.0-SNAPSHOT</version>
   </dependency>
   ```
2. Tabelas numa migração **nova** do serviço:
   ```sql
   create table outbox_event (
       seq                bigint generated always as identity primary key,   -- ordem de publicação
       id                 uuid        not null unique,
       type               varchar(80) not null,
       aggregate_id       uuid        not null,
       aggregate_version  bigint      not null,
       payload            text        not null,
       created_at         timestamp with time zone not null,
       published_at       timestamp with time zone
   );
   create index idx_outbox_unpublished on outbox_event (seq) where published_at is null;
   create table processed_message (id uuid primary key, processed_at timestamp with time zone not null);  -- só quem consome
   ```
3. Configuração no `application.yml`, apontando para as variáveis de ambiente do serviço:
   ```yaml
   socialflux:
     events:
       enabled: ${MEU_SERVICO_EVENTS_ENABLED:true}
       exchange: ${EVENTS_EXCHANGE:socialflux.events}
   ```
4. `@EnableScheduling` na aplicação (para o relay agendado).
5. Para consumir: declare a fila e a ligação com `EventQueues` (condicionadas a `socialflux.events.enabled=true`), um
   `@RabbitListener` e, no handler, `EventEnvelope.parse(...)` + `inbox.once(event.id(), ...)`. Ignore tipos desconhecidos e
   `aggregateVersion` antigos. Exemplo real: `profile_service/.../events/Amqp.java` e `AccountEventHandler.java`.

## Configuração (`socialflux.events.*`)

| Chave | Padrão | Descrição |
| --- | --- | --- |
| `enabled` | `false` | Liga o RabbitMQ (publicador, topologia, relay agendado). Desligado, o outbox continua gravando |
| `exchange` | `socialflux.events` | Exchange `topic`; routing key = tipo do evento (`<domínio>.<evento>.v<n>`) |
| `mandatory` | `false` | Evento sem fila ligada vira falha e fica pendente (exige `spring.rabbitmq.publisher-returns: true`) |
| `relay-interval` | `PT0.5S` | Intervalo do relay |
| `batch-size` | `100` | Eventos por rodada do relay |
| `retention` | `P7D` | Por quanto tempo eventos publicados ficam no outbox |

O serviço também precisa de `spring.rabbitmq.publisher-confirm-type: simple`.

## Build e testes

Na raiz do repositório: `mvn -pl libs/socialflux-events install` (instala no repositório Maven local; os serviços dependem
dela) ou `mvn -pl <serviço> -am test` (compila a biblioteca junto). Os testes usam PostgreSQL real embutido (zonky), sem Docker.
