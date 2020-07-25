package com.redhat.emergency.response.kafka.tracing;

import io.opentracing.References;
import io.opentracing.Span;
import io.opentracing.SpanContext;
import io.opentracing.Tracer;
import io.opentracing.propagation.Format;
import io.opentracing.tag.Tags;
import io.smallrye.reactive.messaging.kafka.IncomingKafkaRecord;

public class TracingKafkaUtils {

    public static Span buildChildSpan(String operationName, IncomingKafkaRecord<String, String> record, Tracer tracer) {

        SpanContext parentContext = extractSpanContext(record, tracer);

        String consumerOperation = "FROM_" + record.getTopic();
        Tracer.SpanBuilder spanBuilder = tracer
                .buildSpan(consumerOperation)
                .withTag(Tags.SPAN_KIND.getKey(), Tags.SPAN_KIND_CONSUMER);

        if (parentContext != null) {
            spanBuilder.addReference(References.FOLLOWS_FROM, parentContext);
        }

        Span span = spanBuilder.start();
        span.setTag("partition", record.getPartition())
                .setTag("topic", record.getTopic())
                .setTag("offset", record.getOffset())
                .setTag(Tags.PEER_SERVICE.getKey(), "kafka");
        span.finish();

        //Create new child span
        return tracer.buildSpan(operationName).ignoreActiveSpan()
                .withTag(Tags.SPAN_KIND.getKey(), Tags.SPAN_KIND_CONSUMER)
                .asChildOf(span).start();
    }

    public static SpanContext extractSpanContext(IncomingKafkaRecord<String, String> record, Tracer tracer) {
        return tracer.extract(Format.Builtin.TEXT_MAP, new HeadersMapExtractAdapter(record.getHeaders()));
    }

}
