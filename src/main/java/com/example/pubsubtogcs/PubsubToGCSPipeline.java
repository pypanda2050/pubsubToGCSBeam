package com.example.pubsubtogcs;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Apache Beam pipeline to stream data from GCP PubSub and write to GCS in CSV format.
 */
public class PubsubToGCSPipeline {
    private static final Logger LOG = LoggerFactory.getLogger(PubsubToGCSPipeline.class);

    /**
     * Pipeline options interface.
     */
    public interface PubsubToGCSOptions extends PipelineOptions {
        @Description("PubSub subscription to read from")
        @Default.String("projects/YOUR_PROJECT/subscriptions/YOUR_SUBSCRIPTION")
        String getInputSubscription();
        void setInputSubscription(String value);

        @Description("GCS bucket to write output files")
        @Default.String("gs://YOUR_BUCKET")
        String getOutputBucket();
        void setOutputBucket(String value);

        @Description("GCS output path prefix")
        @Default.String("output")
        String getOutputPathPrefix();
        void setOutputPathPrefix(String value);
    }

    public static void main(String[] args) {
        PubsubToGCSOptions options = PipelineOptionsFactory.fromArgs(args)
                .withValidation()
                .as(PubsubToGCSOptions.class);

        Pipeline pipeline = Pipeline.create(options);

        // Read messages from PubSub
        PCollection<PubsubMessage> pubsubMessages = pipeline
                .apply("Read from PubSub",
                        PubsubIO.readMessages().fromSubscription(options.getInputSubscription()));

        // Parse Avro messages
        PCollection<Message> messages = pubsubMessages
                .apply("Parse Avro Messages", ParDo.of(new PubsubToAvroParser()))
                .apply("Convert to Message", ParDo.of(new AvroMessageParser()));

        // Write to event_processing (all messages)
        writeEventProcessing(messages, options);

        // Write to event_data (messages with either header or body or both)
        writeEventData(messages, options);

        pipeline.run().waitUntilFinish();
    }

    /**
     * Write all messages to event_processing prefix.
     */
    private static void writeEventProcessing(PCollection<Message> messages, PubsubToGCSOptions options) {
        messages
                .apply("Assign Timestamps", ParDo.of(new DoFn<Message, Message>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        Message msg = c.element();
                        c.outputWithTimestamp(msg, new Instant(msg.getCreateTimestamp()));
                    }
                }))
                .apply("Window for Event Processing", Window.into(FixedWindows.of(Duration.standardMinutes(1))))
                .apply("Format Event Processing CSV", Combine.globally(new FormatCSVCombineFn())
                        .withoutDefaults())
                .apply("Write Event Processing to GCS",
                        org.apache.beam.sdk.io.TextIO.write()
                                .to(options.getOutputBucket() + "/event_processing/")
                                .withWindowedWrites()
                                .withNumShards(1)
                                .withSuffix(".csv"));
    }

    /**
     * Write messages with either header or body (or both) to event_data prefix.
     */
    private static void writeEventData(PCollection<Message> messages, PubsubToGCSOptions options) {
        messages
                .apply("Filter Messages with Header or Body", Filter.by(Message::hasHeaderOrBody))
                .apply("Assign Timestamps for Event Data", ParDo.of(new DoFn<Message, Message>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        Message msg = c.element();
                        c.outputWithTimestamp(msg, new Instant(msg.getCreateTimestamp()));
                    }
                }))
                .apply("Window for Event Data", Window.into(FixedWindows.of(Duration.standardMinutes(1))))
                .apply("Format Event Data CSV", Combine.globally(new FormatCSVCombineFn())
                        .withoutDefaults())
                .apply("Write Event Data to GCS",
                        org.apache.beam.sdk.io.TextIO.write()
                                .to(options.getOutputBucket() + "/event_data/")
                                .withWindowedWrites()
                                .withNumShards(1)
                                .withSuffix(".csv"));
    }

    /**
     * DoFn to parse PubsubMessage to Avro GenericRecord.
     */
    private static class PubsubToAvroParser extends DoFn<PubsubMessage, GenericRecord> {
        /**
         * Avro schema for message records.
         * Constraints:
         * - saga_id: required, non-null string (UUID format)
         * - node_id: required, non-null, non-empty bytes
         * - create_timestamp: required, non-null long
         * - header: optional, nullable bytes
         * - body: optional, nullable bytes
         */
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

        private transient Schema schema;
        private transient DatumReader<GenericRecord> reader;

        @Setup
        public void setup() {
            schema = new Schema.Parser().parse(AVRO_SCHEMA_JSON);
            reader = new GenericDatumReader<>(schema);
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            try {
                PubsubMessage pubsubMessage = c.element();
                byte[] payload = pubsubMessage.getPayload();
                
                if (payload == null || payload.length == 0) {
                    LOG.warn("Received empty payload, skipping");
                    return;
                }
                
                Decoder decoder = DecoderFactory.get().binaryDecoder(payload, null);
                GenericRecord record = reader.read(null, decoder);
                c.output(record);
            } catch (Exception e) {
                LOG.error("Error parsing PubsubMessage to Avro", e);
            }
        }
    }

    /**
     * CombineFn to format messages as CSV with header.
     */
    static class FormatCSVCombineFn extends Combine.CombineFn<Message, StringBuilder, String> {
        @Override
        public StringBuilder createAccumulator() {
            return new StringBuilder("saga_id,node_id,create_timestamp,header,body\n");
        }

        @Override
        public StringBuilder addInput(StringBuilder accumulator, Message input) {
            accumulator.append(input.toCSV()).append("\n");
            return accumulator;
        }

        @Override
        public StringBuilder mergeAccumulators(Iterable<StringBuilder> accumulators) {
            StringBuilder merged = new StringBuilder("saga_id,node_id,create_timestamp,header,body\n");
            for (StringBuilder acc : accumulators) {
                // Skip the header from subsequent accumulators
                String content = acc.toString();
                int headerEnd = content.indexOf('\n');
                if (headerEnd >= 0) {
                    merged.append(content.substring(headerEnd + 1));
                } else {
                    merged.append(content);
                }
            }
            return merged;
        }

        @Override
        public String extractOutput(StringBuilder accumulator) {
            return accumulator.toString();
        }
    }
}

