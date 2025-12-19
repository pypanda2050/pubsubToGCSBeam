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
            if (message != null) {
                c.output(message);
            }
        } catch (Exception e) {
            LOG.error("Error parsing Avro record: " + e.getMessage(), e);
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
            String sagaIdStr = null;
            if (sagaIdObj instanceof String) {
                sagaIdStr = (String) sagaIdObj;
            } else if (sagaIdObj instanceof CharSequence) {
                sagaIdStr = sagaIdObj.toString();
            } else if (sagaIdObj instanceof UUID) {
                sagaId = (UUID) sagaIdObj;
            }
            
            if (sagaIdStr != null && !sagaIdStr.isEmpty()) {
                try {
                    sagaId = UUID.fromString(sagaIdStr);
                } catch (IllegalArgumentException e) {
                    LOG.warn("Invalid UUID format: " + sagaIdStr, e);
                }
            }
        }

        // Parse node_id (byte[]) - must be non-empty
        Object nodeIdObj = record.get("node_id");
        if (nodeIdObj != null) {
            if (nodeIdObj instanceof ByteBuffer) {
                ByteBuffer buffer = (ByteBuffer) nodeIdObj;
                // Duplicate to avoid mutating the original buffer
                ByteBuffer duplicate = buffer.duplicate();
                nodeId = new byte[duplicate.remaining()];
                duplicate.get(nodeId);
            } else if (nodeIdObj instanceof byte[]) {
                nodeId = (byte[]) nodeIdObj;
            }
            
            // Validate that node_id is not empty
            if (nodeId != null && nodeId.length == 0) {
                LOG.error("node_id cannot be empty. Skipping message with saga_id: " + 
                        (sagaId != null ? sagaId.toString() : "unknown"));
                return null;
            }
        } else {
            LOG.error("node_id is required but was null. Skipping message.");
            return null;
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
                // Duplicate to avoid mutating the original buffer
                ByteBuffer duplicate = buffer.duplicate();
                header = new byte[duplicate.remaining()];
                duplicate.get(header);
            } else if (headerObj instanceof byte[]) {
                header = (byte[]) headerObj;
            }
        }

        // Parse body (byte[])
        Object bodyObj = record.get("body");
        if (bodyObj != null) {
            if (bodyObj instanceof ByteBuffer) {
                ByteBuffer buffer = (ByteBuffer) bodyObj;
                // Duplicate to avoid mutating the original buffer
                ByteBuffer duplicate = buffer.duplicate();
                body = new byte[duplicate.remaining()];
                duplicate.get(body);
            } else if (bodyObj instanceof byte[]) {
                body = (byte[]) bodyObj;
            }
        }

        return new Message(sagaId, nodeId, createTimestamp, header, body);
    }
}

