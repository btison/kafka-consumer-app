package com.redhat.emergency.response.kafka;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Optional;
import javax.enterprise.context.ApplicationScoped;

import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import io.smallrye.reactive.messaging.kafka.IncomingKafkaRecord;
import io.smallrye.reactive.messaging.kafka.IncomingKafkaRecordMetadata;
import org.apache.kafka.common.header.Headers;
import org.eclipse.microprofile.reactive.messaging.Acknowledgment;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ApplicationScoped
public class KafkaRecordConsumer {

    private static final Logger log = LoggerFactory.getLogger(KafkaRecordConsumer.class);

    @Incoming("channel")
    @Acknowledgment(Acknowledgment.Strategy.PRE_PROCESSING)
    public Uni<Void> processUni(IncomingKafkaRecord<String, String> message) {
        return Uni.createFrom().item(message).emitOn(Infrastructure.getDefaultExecutor())
                .onItem().transform(record -> {
                    Optional<IncomingKafkaRecordMetadata> metadata = message.getMetadata(IncomingKafkaRecordMetadata.class);
                    StringBuilder sb = new StringBuilder();
                    metadata.ifPresent(m -> {
                        Headers headers = m.getHeaders();
                        Arrays.stream(headers.toArray()).forEach(header -> sb.append(header.key()).append(": ").append(convertToString(header.key(), header.value())).append(", "));
                    });
                    String hs = sb.toString();
                    if (hs.length() > 2) {
                        hs = hs.substring(0, hs.length() - 2);
                    }
                    log.info("Consumed message from topic '" + message.getTopic() + "', partition '"
                            + message.getPartition() + "', offset '" + message.getOffset() + "'");
                    log.info("    Headers: " + hs);
                    log.info("    Message key: " + message.getKey());
                    log.info("    Message value: " + message.getPayload());
                    return null;
                });
    }

    private String convertToString(String key, byte[] bytes) {
        if ("apicurio.globalId".equalsIgnoreCase(key)) {
            return Long.toString(ByteBuffer.wrap(bytes).getLong());
        } else {
            return new String(bytes);
        }
    }

}
