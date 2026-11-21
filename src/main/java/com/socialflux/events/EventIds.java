package com.socialflux.events;

import java.nio.ByteBuffer;
import java.security.SecureRandom;
import java.time.Clock;
import java.util.UUID;

/** Gerador de UUIDv7 (RFC 9562) para ids de eventos e de agregados: ordenável no tempo. */
public final class EventIds {

    private static final SecureRandom RANDOM = new SecureRandom();

    private EventIds() {
    }

    public static UUID v7(Clock clock) {
        long timestamp = clock.millis();
        byte[] random = new byte[10];
        RANDOM.nextBytes(random);

        long msb = (timestamp << 16)
                | (0x7L << 12)
                | (((random[0] & 0x0FL) << 8) | (random[1] & 0xFFL));
        long lsb = 0x8000000000000000L
                | (ByteBuffer.wrap(random, 2, 8).getLong() & 0x3FFFFFFFFFFFFFFFL);
        return new UUID(msb, lsb);
    }
}
