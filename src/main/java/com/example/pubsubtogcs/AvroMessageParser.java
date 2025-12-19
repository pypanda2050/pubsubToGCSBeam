package com.example.pubsubtogcs;

import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.transforms.DoFn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.UUID;

/**
 * DoFn to parse Avro GenericRecord to Message object.
 */
public class AvroMessageParser extends DoFn<GenericRecord, Message> {
    private static final Logger LOG = LoggerFactory.getLogger(AvroMessageParser.class);

    @ProcessElement
    public void processElement(ProcessContext c) {
        try {
            GenericRecord record = c.element();
            Message message = parseAvroRecord(record);
            c.output(message);
        } catch (Exception e) {
            LOG.error("Error parsing Avro record", e);
            // Optionally, you can output to a dead letter queue or handle errors differently
        }
    }

    private Message parseAvroRecord(GenericRecord record) {
        UUID sagaId = null;
        byte[] nodeId = null;
        long createTimestamp = 0L;
        byte[] header = null;
        byte[] body = null;

        // Parse saga_id (UUID)
        Object sagaIdObj = record.get("saga_id");
        if (sagaIdObj != null) {
            if (sagaIdObj instanceof String) {
                sagaId = UUID.fromString((String) sagaIdObj);
            } else if (sagaIdObj instanceof UUID) {
                sagaId = (UUID) sagaIdObj;
            }
        }

        // Parse node_id (byte[])
        Object nodeIdObj = record.get("node_id");
        if (nodeIdObj != null) {
            if (nodeIdObj instanceof ByteBuffer) {
                ByteBuffer buffer = (ByteBuffer) nodeIdObj;
                nodeId = new byte[buffer.remaining()];
                buffer.get(nodeId);
            } else if (nodeIdObj instanceof byte[]) {
                nodeId = (byte[]) nodeIdObj;
            }
        }

        // Parse create_timestamp (long)
        Object timestampObj = record.get("create_timestamp");
        if (timestampObj != null) {
            if (timestampObj instanceof Long) {
                createTimestamp = (Long) timestampObj;
            } else if (timestampObj instanceof Number) {
                createTimestamp = ((Number) timestampObj).longValue();
            }
        }

        // Parse header (byte[])
        Object headerObj = record.get("header");
        if (headerObj != null) {
            if (headerObj instanceof ByteBuffer) {
                ByteBuffer buffer = (ByteBuffer) headerObj;
                header = new byte[buffer.remaining()];
                buffer.get(header);
            } else if (headerObj instanceof byte[]) {
                header = (byte[]) headerObj;
            }
        }

        // Parse body (byte[])
        Object bodyObj = record.get("body");
        if (bodyObj != null) {
            if (bodyObj instanceof ByteBuffer) {
                ByteBuffer buffer = (ByteBuffer) bodyObj;
                body = new byte[buffer.remaining()];
                buffer.get(body);
            } else if (bodyObj instanceof byte[]) {
                body = (byte[]) bodyObj;
            }
        }

        return new Message(sagaId, nodeId, createTimestamp, header, body);
    }
}

