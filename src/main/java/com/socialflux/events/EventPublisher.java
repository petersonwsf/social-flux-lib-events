package com.socialflux.events;

import java.util.UUID;

/** Porta de saída dos eventos (RabbitMQ em produção; substituível nos testes por um bean {@code @Primary}). */
public interface EventPublisher {

    /**
     * Publica uma mensagem já serializada e só retorna depois da confirmação do broker.
     * Deve lançar exceção em caso de falha (inclusive "nenhuma fila ligada" com {@code mandatory}) para o evento
     * continuar pendente no outbox.
     */
    void publish(UUID messageId, String routingKey, String json);
}
