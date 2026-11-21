# Arquitetura — socialflux-events

Biblioteca (jar, sem `main`) com a infraestrutura de eventos compartilhada pelos serviços Spring Boot. Registrada por
auto-configuração do Spring Boot; cada serviço continua dono das suas tabelas, filas e handlers. Uso e configuração: [`../README.md`](../README.md).

```
serviço ── Outbox.add() (mesma transação da mudança) ─► outbox_event
                                                          │
                     OutboxRelay (agendado) ──────────────┘─► EventPublisher ─► AmqpEventPublisher ─► RabbitMQ (socialflux.events)
RabbitMQ ─► @RabbitListener do serviço ─► EventEnvelope.parse ─► Inbox.once(id, efeito) ─► processed_message
```

## Árvore de diretórios

```
libs/socialflux-events/
├── pom.xml                       parent spring-boot-starter-parent 4.1.2; dependências: starter-jdbc, starter-amqp, starter-jackson; testes: embedded-postgres
├── .gitignore                    ignora target/ e arquivos de IDE
├── README.md                     o que a biblioteca entrega, como usar, configuração
├── docs/ARCHITECTURE.md          este arquivo
└── src/
    ├── main/java/com/socialflux/events/
    ├── main/resources/META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports
    └── test/java/com/socialflux/events/
```

## Arquivos (`com.socialflux.events`)

| Arquivo | O que representa |
| --- | --- |
| `SocialFluxEventsAutoConfiguration.java` | Auto-configuração: `Outbox`, `OutboxRelay`, `Inbox` e `Clock` padrão sempre; publicador RabbitMQ e topologia (exchange, DLX, fila de mortos) só com `socialflux.events.enabled=true` |
| `SocialFluxEventsProperties.java` | Propriedades `socialflux.events.*` (`enabled`, `exchange`, `mandatory`, `relay-interval`, `batch-size`, `retention`), validadas ao subir |
| `Outbox.java` | Grava o envelope do evento em `outbox_event` na transação ativa; lê/trava pendentes, marca publicados, expurga, conta pendentes |
| `OutboxRelay.java` | Publica os pendentes em ordem e em lotes (para no primeiro erro para não pular eventos) e expurga os antigos |
| `EventPublisher.java` | Porta de saída: publica e só retorna após a confirmação do broker; exceção = evento continua pendente |
| `AmqpEventPublisher.java` | Implementação RabbitMQ: mensagem persistente, *publisher confirms* e `mandatory` opcional |
| `Inbox.java` | Idempotência de consumidor via `processed_message`, na mesma transação do efeito |
| `EventEnvelope.java` | Leitura do envelope pelos consumidores (campos obrigatórios validados; `data` como árvore JSON) |
| `EventQueues.java` | Nomes da topologia (bean do exchange, DLX, fila de mortos) e atalhos para declarar fila durável com DLX e ligação |
| `EventIds.java` | Gerador de UUIDv7 (`EventIds.v7(clock)`) para ids de eventos e de agregados dos serviços |

## Testes (`src/test`)

| Arquivo | O que cobre |
| --- | --- |
| `SocialFluxEventsTest.java` | Outbox exige transação; relay publica em ordem com o envelope correto; falha de publicação mantém pendente e a ordem; inbox executa uma vez e não registra se o efeito falha; envelope inválido; beans do broker só com `enabled=true`; configuração inválida falha ao subir |
