package com.example.pubsubtogcs;

import com.example.pubsubtogcs.PubsubToGCSPipeline.PubsubToAvroParser;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessageWithAttributesCoder;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTagList;
import org.junit.Rule;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.util.Arrays;
import java.util.UUID;

import static org.junit.Assert.*;

/**
 * Unit tests for PubsubToGCSPipeline.
 */
public class PubsubToGCSPipelineTest {

        @Rule
        public TestPipeline pipeline = TestPipeline.create();

        @Test
        public void testFilterMessagesWithHeaderOrBody() {
                UUID sagaId1 = UUID.randomUUID();
                UUID sagaId2 = UUID.randomUUID();
                UUID sagaId3 = UUID.randomUUID();
                UUID sagaId4 = UUID.randomUUID();
                UUID sagaId5 = UUID.randomUUID();

                Message msg1 = new Message(sagaId1, null, 1234567890L, "header1".getBytes(), "body1".getBytes());
                Message msg2 = new Message(sagaId2, null, 1234567890L, null, "body2".getBytes());
                Message msg3 = new Message(sagaId3, null, 1234567890L, "header3".getBytes(), null);
                Message msg4 = new Message(sagaId4, null, 1234567890L, "header4".getBytes(), "body4".getBytes());
                Message msg5 = new Message(sagaId5, null, 1234567890L, null, null);

                PCollection<Message> input = pipeline.apply(
                                Create.of(msg1, msg2, msg3, msg4, msg5)
                                                .withCoder(org.apache.beam.sdk.coders.SerializableCoder
                                                                .of(Message.class)));
                PCollection<Message> filtered = input.apply("Filter Messages with Header or Body",
                                Filter.by(Message::hasHeaderOrBody));

                PAssert.that(filtered).satisfies(messages -> {
                        int count = 0;
                        for (Message msg : messages) {
                                assertTrue(msg.hasHeaderOrBody());
                                // Should include msg1 (both), msg2 (body only), msg3 (header only), msg4 (both)
                                // Should NOT include msg5 (neither)
                                assertTrue(msg.getSagaId().equals(sagaId1) ||
                                                msg.getSagaId().equals(sagaId2) ||
                                                msg.getSagaId().equals(sagaId3) ||
                                                msg.getSagaId().equals(sagaId4));
                                assertFalse(msg.getSagaId().equals(sagaId5));
                                count++;
                        }
                        assertEquals(4, count);
                        return null;
                });

                pipeline.run();
        }

        @Test
        public void testFormatCSVCombineFn() {
                PubsubToGCSPipeline.FormatCSVCombineFn combineFn = new PubsubToGCSPipeline.FormatCSVCombineFn();

                UUID sagaId1 = UUID.fromString("550e8400-e29b-41d4-a716-446655440000");
                UUID sagaId2 = UUID.fromString("660e8400-e29b-41d4-a716-446655440001");

                Message msg1 = new Message(sagaId1, "node1".getBytes(), 1234567890L,
                                "header1".getBytes(), "body1".getBytes());
                Message msg2 = new Message(sagaId2, "node2".getBytes(), 1234567891L,
                                "header2".getBytes(), "body2".getBytes());

                StringBuilder acc1 = combineFn.createAccumulator();
                acc1 = combineFn.addInput(acc1, msg1);

                StringBuilder acc2 = combineFn.createAccumulator();
                acc2 = combineFn.addInput(acc2, msg2);

                StringBuilder merged = combineFn.mergeAccumulators(Arrays.asList(acc1, acc2));
                String output = combineFn.extractOutput(merged);

                assertTrue(output.contains("saga_id,node_id,create_timestamp,header,body"));
                assertTrue(output.contains("550e8400-e29b-41d4-a716-446655440000"));
                assertTrue(output.contains("660e8400-e29b-41d4-a716-446655440001"));
                assertTrue(output.contains("node1"));
                assertTrue(output.contains("node2"));
                assertTrue(output.contains("header1"));
                assertTrue(output.contains("body2"));
        }

        @Test
        public void testFormatCSVCombineFn_SingleMessage() {
                PubsubToGCSPipeline.FormatCSVCombineFn combineFn = new PubsubToGCSPipeline.FormatCSVCombineFn();

                UUID sagaId = UUID.randomUUID();
                Message msg = new Message(sagaId, "node".getBytes(), 1234567890L,
                                "header".getBytes(), "body".getBytes());

                StringBuilder acc = combineFn.createAccumulator();
                acc = combineFn.addInput(acc, msg);
                String output = combineFn.extractOutput(acc);

                assertTrue(output.startsWith("saga_id,node_id,create_timestamp,header,body"));
                assertTrue(output.contains(sagaId.toString()));
                assertTrue(output.contains("node"));
                assertTrue(output.contains("header"));
                assertTrue(output.contains("body"));
        }

        @Test
        public void testFormatCSVCombineFn_EmptyAccumulator() {
                PubsubToGCSPipeline.FormatCSVCombineFn combineFn = new PubsubToGCSPipeline.FormatCSVCombineFn();

                StringBuilder acc = combineFn.createAccumulator();
                String output = combineFn.extractOutput(acc);

                assertEquals("saga_id,node_id,create_timestamp,header,body\n", output);
        }

        @Test
        public void testPipelineOptions() {
                String[] args = new String[] {
                                "--inputSubscription=projects/test/subscriptions/test-sub",
                                "--outputBucket=gs://test-bucket"
                };

                PubsubToGCSPipeline.PubsubToGCSOptions options = PipelineOptionsFactory.fromArgs(args)
                                .as(PubsubToGCSPipeline.PubsubToGCSOptions.class);

                assertEquals("projects/test/subscriptions/test-sub", options.getInputSubscription());
                assertEquals("gs://test-bucket", options.getOutputBucket());
        }

        @Test
        public void testPipelineOptions_DefaultValues() {
                PubsubToGCSPipeline.PubsubToGCSOptions options = PipelineOptionsFactory.create()
                                .as(PubsubToGCSPipeline.PubsubToGCSOptions.class);

                assertEquals("projects/YOUR_PROJECT/subscriptions/YOUR_SUBSCRIPTION",
                                options.getInputSubscription());
                assertEquals("gs://YOUR_BUCKET", options.getOutputBucket());
                assertEquals("output", options.getOutputPathPrefix());
        }

        @Test
        public void testPubsubToAvroParser_Success() throws Exception {
                // Create valid Avro record
                String schemaJson = "{\n" +
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
                Schema schema = new Schema.Parser().parse(schemaJson);
                ByteArrayOutputStream out = new ByteArrayOutputStream();
                Encoder encoder = EncoderFactory.get().binaryEncoder(out, null);
                GenericDatumWriter<Object> writer = new GenericDatumWriter<>(schema);

                writer.write(new GenericRecordBuilder(schema)
                                .set("saga_id", UUID.randomUUID().toString())
                                .set("node_id", java.nio.ByteBuffer.wrap("node".getBytes()))
                                .set("create_timestamp", 12345L)
                                .build(), encoder);
                encoder.flush();

                PubsubMessage message = new PubsubMessage(out.toByteArray(), null);

                PCollectionTuple results = pipeline
                                .apply("Create", Create.of(message))
                                .apply("Parse", ParDo.of(new PubsubToAvroParser())
                                                .withOutputTags(PubsubToAvroParser.SUCCESS_TAG,
                                                                TupleTagList.of(PubsubToAvroParser.FAILURE_TAG)));

                PCollection<GenericRecord> success = results.get(PubsubToAvroParser.SUCCESS_TAG);
                PCollection<PubsubMessage> failure = results.get(PubsubToAvroParser.FAILURE_TAG);

                success.setCoder(AvroCoder.of(schema));
                failure.setCoder(PubsubMessageWithAttributesCoder.of());

                PAssert.that(success).satisfies(records -> {
                        int count = 0;
                        for (Object record : records) {
                                count++;
                        }
                        assertEquals(1, count);
                        return null;
                });

                PAssert.that(failure).empty();

                pipeline.run();
        }

        @Test
        public void testPubsubToAvroParser_Failure_Corrupt() {
                PubsubMessage message = new PubsubMessage("invalid avro bytes".getBytes(), null);

                PCollectionTuple results = pipeline
                                .apply("Create", Create.of(message))
                                .apply("Parse", ParDo.of(new PubsubToAvroParser())
                                                .withOutputTags(PubsubToAvroParser.SUCCESS_TAG,
                                                                TupleTagList.of(PubsubToAvroParser.FAILURE_TAG)));

                // Define schema for coder
                String schemaJson = "{\n" +
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
                Schema schema = new Schema.Parser().parse(schemaJson);

                results.get(PubsubToAvroParser.SUCCESS_TAG).setCoder(AvroCoder.of(schema));
                results.get(PubsubToAvroParser.FAILURE_TAG).setCoder(PubsubMessageWithAttributesCoder.of());

                PAssert.that(results.get(PubsubToAvroParser.SUCCESS_TAG)).empty();

                byte[] expectedPayload = "invalid avro bytes".getBytes();
                PAssert.that(results.get(PubsubToAvroParser.FAILURE_TAG)).satisfies(messages -> {
                        int count = 0;
                        for (PubsubMessage msg : messages) {
                                assertArrayEquals(expectedPayload, msg.getPayload());
                                count++;
                        }
                        assertEquals(1, count);
                        return null;
                });

                pipeline.run();
        }

        @Test
        public void testPubsubToAvroParser_Failure_Empty() {
                PubsubMessage message = new PubsubMessage(new byte[0], null);

                PCollectionTuple results = pipeline
                                .apply("Create", Create.of(message))
                                .apply("Parse", ParDo.of(new PubsubToAvroParser())
                                                .withOutputTags(PubsubToAvroParser.SUCCESS_TAG,
                                                                TupleTagList.of(PubsubToAvroParser.FAILURE_TAG)));

                // Define schema for coder
                String schemaJson = "{\n" +
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
                Schema schema = new Schema.Parser().parse(schemaJson);

                results.get(PubsubToAvroParser.SUCCESS_TAG).setCoder(AvroCoder.of(schema));
                results.get(PubsubToAvroParser.FAILURE_TAG).setCoder(PubsubMessageWithAttributesCoder.of());

                PAssert.that(results.get(PubsubToAvroParser.SUCCESS_TAG)).empty();

                byte[] expectedPayload = new byte[0];
                PAssert.that(results.get(PubsubToAvroParser.FAILURE_TAG)).satisfies(messages -> {
                        int count = 0;
                        for (PubsubMessage msg : messages) {
                                assertArrayEquals(expectedPayload, msg.getPayload());
                                count++;
                        }
                        assertEquals(1, count);
                        return null;
                });

                pipeline.run();
        }
}
