package com.example.pubsubtogcs;

import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;

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
                        .withCoder(org.apache.beam.sdk.coders.SerializableCoder.of(Message.class)));
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
        PubsubToGCSPipeline.FormatCSVCombineFn combineFn = 
                new PubsubToGCSPipeline.FormatCSVCombineFn();

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
        PubsubToGCSPipeline.FormatCSVCombineFn combineFn = 
                new PubsubToGCSPipeline.FormatCSVCombineFn();

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
        PubsubToGCSPipeline.FormatCSVCombineFn combineFn = 
                new PubsubToGCSPipeline.FormatCSVCombineFn();

        StringBuilder acc = combineFn.createAccumulator();
        String output = combineFn.extractOutput(acc);

        assertEquals("saga_id,node_id,create_timestamp,header,body\n", output);
    }

    @Test
    public void testPipelineOptions() {
        String[] args = new String[]{
                "--inputSubscription=projects/test/subscriptions/test-sub",
                "--outputBucket=gs://test-bucket"
        };

        PubsubToGCSPipeline.PubsubToGCSOptions options = 
                PipelineOptionsFactory.fromArgs(args)
                        .as(PubsubToGCSPipeline.PubsubToGCSOptions.class);

        assertEquals("projects/test/subscriptions/test-sub", options.getInputSubscription());
        assertEquals("gs://test-bucket", options.getOutputBucket());
    }

    @Test
    public void testPipelineOptions_DefaultValues() {
        PubsubToGCSPipeline.PubsubToGCSOptions options = 
                PipelineOptionsFactory.create()
                        .as(PubsubToGCSPipeline.PubsubToGCSOptions.class);

        assertEquals("projects/YOUR_PROJECT/subscriptions/YOUR_SUBSCRIPTION", 
                options.getInputSubscription());
        assertEquals("gs://YOUR_BUCKET", options.getOutputBucket());
        assertEquals("output", options.getOutputPathPrefix());
    }
}

