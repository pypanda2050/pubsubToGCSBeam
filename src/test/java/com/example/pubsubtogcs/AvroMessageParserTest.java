package com.example.pubsubtogcs;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.UUID;

import static org.junit.Assert.*;

/**
 * Unit tests for AvroMessageParser.
 * Note: These tests use a simplified approach to test the parser logic.
 */
public class AvroMessageParserTest {

    private static final String AVRO_SCHEMA_JSON = "{\n" +
            "  \"type\": \"record\",\n" +
            "  \"name\": \"MessageRecord\",\n" +
            "  \"fields\": [\n" +
            "    {\"name\": \"saga_id\", \"type\": \"string\"},\n" +
            "    {\"name\": \"node_id\", \"type\": \"bytes\"},\n" +
            "    {\"name\": \"create_timestamp\", \"type\": \"long\"},\n" +
            "    {\"name\": \"header\", \"type\": [\"null\", \"bytes\"], \"default\": null},\n" +
            "    {\"name\": \"body\", \"type\": [\"null\", \"bytes\"], \"default\": null}\n" +
            "  ]\n" +
            "}";

    @Rule
    public TestPipeline pipeline = TestPipeline.create();

    @Test
    public void testParseAvroRecord_CompleteMessage() {
        Schema schema = new Schema.Parser().parse(AVRO_SCHEMA_JSON);
        GenericRecord record = new GenericData.Record(schema);
        
        UUID sagaId = UUID.fromString("550e8400-e29b-41d4-a716-446655440000");
        byte[] nodeId = "node123".getBytes();
        long timestamp = 1234567890L;
        byte[] header = "header".getBytes();
        byte[] body = "body".getBytes();

        record.put("saga_id", sagaId.toString());
        record.put("node_id", ByteBuffer.wrap(nodeId));
        record.put("create_timestamp", timestamp);
        record.put("header", ByteBuffer.wrap(header));
        record.put("body", ByteBuffer.wrap(body));

        PCollection<GenericRecord> input = pipeline.apply(
                Create.of(record).withCoder(org.apache.beam.sdk.coders.AvroCoder.of(schema)));
        PCollection<Message> output = input.apply(ParDo.of(new AvroMessageParser()));

        PAssert.that(output).satisfies(messages -> {
            Message message = messages.iterator().next();
            assertEquals(sagaId, message.getSagaId());
            assertArrayEquals(nodeId, message.getNodeId());
            assertEquals(timestamp, message.getCreateTimestamp());
            assertArrayEquals(header, message.getHeader());
            assertArrayEquals(body, message.getBody());
            return null;
        });

        pipeline.run();
    }

    @Test
    public void testParseAvroRecord_WithNullFields() {
        Schema schema = new Schema.Parser().parse(AVRO_SCHEMA_JSON);
        GenericRecord record = new GenericData.Record(schema);
        
        String sagaIdStr = "550e8400-e29b-41d4-a716-446655440000";
        byte[] nodeId = "node1".getBytes();
        long timestamp = 1234567890L;

        record.put("saga_id", sagaIdStr);
        record.put("node_id", ByteBuffer.wrap(nodeId));
        record.put("create_timestamp", timestamp);
        record.put("header", null);
        record.put("body", null);

        PCollection<GenericRecord> input = pipeline.apply(
                Create.of(record).withCoder(org.apache.beam.sdk.coders.AvroCoder.of(schema)));
        PCollection<Message> output = input.apply(ParDo.of(new AvroMessageParser()));

        PAssert.that(output).satisfies(messages -> {
            Message message = messages.iterator().next();
            assertEquals(UUID.fromString(sagaIdStr), message.getSagaId());
            assertArrayEquals(nodeId, message.getNodeId());
            assertEquals(timestamp, message.getCreateTimestamp());
            assertNull(message.getHeader());
            assertNull(message.getBody());
            return null;
        });

        pipeline.run();
    }

    @Test
    public void testParseAvroRecord_StringSagaId() {
        Schema schema = new Schema.Parser().parse(AVRO_SCHEMA_JSON);
        GenericRecord record = new GenericData.Record(schema);
        
        String sagaIdStr = "550e8400-e29b-41d4-a716-446655440000";
        byte[] nodeId = "node2".getBytes();
        long timestamp = 1234567890L;

        record.put("saga_id", sagaIdStr);
        record.put("node_id", ByteBuffer.wrap(nodeId));
        record.put("create_timestamp", timestamp);
        record.put("header", null);
        record.put("body", null);

        PCollection<GenericRecord> input = pipeline.apply(
                Create.of(record).withCoder(org.apache.beam.sdk.coders.AvroCoder.of(schema)));
        PCollection<Message> output = input.apply(ParDo.of(new AvroMessageParser()));

        PAssert.that(output).satisfies(messages -> {
            Message message = messages.iterator().next();
            assertEquals(UUID.fromString(sagaIdStr), message.getSagaId());
            return null;
        });

        pipeline.run();
    }

    @Test
    public void testParseAvroRecord_ByteArrayFields() {
        Schema schema = new Schema.Parser().parse(AVRO_SCHEMA_JSON);
        GenericRecord record = new GenericData.Record(schema);
        
        String sagaIdStr = "550e8400-e29b-41d4-a716-446655440000";
        byte[] nodeId = "node".getBytes();
        byte[] header = "header".getBytes();
        byte[] body = "body".getBytes();
        long timestamp = 1234567890L;

        record.put("saga_id", sagaIdStr);
        record.put("node_id", ByteBuffer.wrap(nodeId));
        record.put("create_timestamp", timestamp);
        record.put("header", ByteBuffer.wrap(header));
        record.put("body", ByteBuffer.wrap(body));

        PCollection<GenericRecord> input = pipeline.apply(
                Create.of(record).withCoder(org.apache.beam.sdk.coders.AvroCoder.of(schema)));
        PCollection<Message> output = input.apply(ParDo.of(new AvroMessageParser()));

        PAssert.that(output).satisfies(messages -> {
            Message message = messages.iterator().next();
            assertArrayEquals(nodeId, message.getNodeId());
            assertArrayEquals(header, message.getHeader());
            assertArrayEquals(body, message.getBody());
            return null;
        });

        pipeline.run();
    }

    @Test
    public void testParseAvroRecord_IntegerTimestamp() {
        Schema schema = new Schema.Parser().parse(AVRO_SCHEMA_JSON);
        GenericRecord record = new GenericData.Record(schema);
        
        String sagaIdStr = "550e8400-e29b-41d4-a716-446655440000";
        byte[] nodeId = "node3".getBytes();
        int timestamp = 1234567890;

        record.put("saga_id", sagaIdStr);
        record.put("node_id", ByteBuffer.wrap(nodeId));
        record.put("create_timestamp", timestamp);
        record.put("header", null);
        record.put("body", null);

        PCollection<GenericRecord> input = pipeline.apply(
                Create.of(record).withCoder(org.apache.beam.sdk.coders.AvroCoder.of(schema)));
        PCollection<Message> output = input.apply(ParDo.of(new AvroMessageParser()));

        PAssert.that(output).satisfies(messages -> {
            Message message = messages.iterator().next();
            assertEquals(1234567890L, message.getCreateTimestamp());
            return null;
        });

        pipeline.run();
    }

    @Test
    public void testParseAvroRecord_EmptyByteArrays() {
        Schema schema = new Schema.Parser().parse(AVRO_SCHEMA_JSON);
        GenericRecord record = new GenericData.Record(schema);
        
        String sagaIdStr = "550e8400-e29b-41d4-a716-446655440000";
        byte[] nodeId = "node4".getBytes();  // node_id must be non-empty
        byte[] emptyHeader = new byte[0];
        byte[] emptyBody = new byte[0];
        long timestamp = 1234567890L;

        record.put("saga_id", sagaIdStr);
        record.put("node_id", ByteBuffer.wrap(nodeId));
        record.put("create_timestamp", timestamp);
        record.put("header", ByteBuffer.wrap(emptyHeader));
        record.put("body", ByteBuffer.wrap(emptyBody));

        PCollection<GenericRecord> input = pipeline.apply(
                Create.of(record).withCoder(org.apache.beam.sdk.coders.AvroCoder.of(schema)));
        PCollection<Message> output = input.apply(ParDo.of(new AvroMessageParser()));

        PAssert.that(output).satisfies(messages -> {
            Message message = messages.iterator().next();
            assertArrayEquals(nodeId, message.getNodeId());
            assertArrayEquals(emptyHeader, message.getHeader());
            assertArrayEquals(emptyBody, message.getBody());
            assertFalse(message.hasHeaderOrBody());
            return null;
        });

        pipeline.run();
    }

    @Test
    public void testParseAvroRecord_EmptyNodeId_ShouldBeRejected() {
        Schema schema = new Schema.Parser().parse(AVRO_SCHEMA_JSON);
        GenericRecord record = new GenericData.Record(schema);
        
        String sagaIdStr = "550e8400-e29b-41d4-a716-446655440000";
        byte[] emptyNodeId = new byte[0];
        long timestamp = 1234567890L;

        record.put("saga_id", sagaIdStr);
        record.put("node_id", ByteBuffer.wrap(emptyNodeId));
        record.put("create_timestamp", timestamp);
        record.put("header", null);
        record.put("body", null);

        PCollection<GenericRecord> input = pipeline.apply(
                Create.of(record).withCoder(org.apache.beam.sdk.coders.AvroCoder.of(schema)));
        PCollection<Message> output = input.apply(ParDo.of(new AvroMessageParser()));

        // Empty node_id should be rejected, so output should be empty
        PAssert.that(output).empty();

        pipeline.run();
    }
}
