package com.socialflux.events.logging;

import org.slf4j.MDC;

import java.util.UUID;
import java.util.regex.Pattern;

/**
 * Id de correlação: um identificador que acompanha uma ação do usuário por TODOS os serviços (HTTP entre serviços e eventos
 * do RabbitMQ), para achar no Loki/Grafana todas as linhas de log de um mesmo fluxo.
 * <p>O id vive no MDC do SLF4J (chave {@value #MDC_KEY}), por isso entra sozinho em cada linha de log. Use
 * {@link #open(String)} em um {@code try-with-resources} sempre que iniciar um fluxo fora de uma requisição HTTP (consumidor
 * de evento, rotina agendada); a requisição HTTP já é coberta pelo {@link CorrelationIdFilter}.
 */
public final class CorrelationId {

    /** Cabeçalho HTTP trocado entre gateway, serviços e navegador. */
    public static final String HEADER = "X-Correlation-Id";
    /** Cabeçalho da mensagem AMQP (o id também vai dentro do envelope do evento). */
    public static final String AMQP_HEADER = "x-correlation-id";
    /** Chave do MDC (aparece como campo {@code correlationId} nos logs JSON). */
    public static final String MDC_KEY = "correlationId";

    /** Valor vindo de fora só é aceito neste formato: evita injeção de linhas de log (CR/LF) e valores gigantes. */
    private static final Pattern VALID = Pattern.compile("[A-Za-z0-9._-]{8,64}");

    private CorrelationId() {
    }

    /** Id do fluxo atual, ou {@code null} se não há nenhum. */
    public static String current() {
        return MDC.get(MDC_KEY);
    }

    public static String newId() {
        return UUID.randomUUID().toString();
    }

    public static boolean isValid(String value) {
        return value != null && VALID.matcher(value).matches();
    }

    /**
     * Abre um escopo com o id informado (ou um novo, se vier vazio/inválido) e devolve o que fechar o escopo,
     * restaurando o id anterior.
     */
    public static Scope open(String id) {
        String previous = MDC.get(MDC_KEY);
        MDC.put(MDC_KEY, isValid(id) ? id : newId());
        return new Scope(previous);
    }

    /** Restaura o id anterior do MDC ao fechar. */
    public static final class Scope implements AutoCloseable {

        private final String previous;

        private Scope(String previous) {
            this.previous = previous;
        }

        @Override
        public void close() {
            if (previous == null) {
                MDC.remove(MDC_KEY);
            } else {
                MDC.put(MDC_KEY, previous);
            }
        }
    }
}
