# PubSub to GCS Beam Pipeline

Apache Beam pipeline to stream data from GCP PubSub (Avro format) and write to GCS in CSV format.

## Overview

This pipeline reads messages from Google Cloud Pub/Sub in Avro format and writes them to Google Cloud Storage (GCS) in CSV format. The messages contain:
- `saga_id` (UUID)
- `node_id` (byte[])
- `create_timestamp` (long)
- `header` (byte[])
- `body` (byte[])

The pipeline creates two types of output files:
1. **event_processing**: All messages are written to files under the `event_processing/` prefix, with filenames appended with the `create_timestamp` truncated to minute precision.
2. **event_data**: Messages where either `header` or `body` (or both) are not empty are written to files under the `event_data/` prefix, with filenames appended with the `create_timestamp` truncated to minute precision.

## Prerequisites

- Java 11 or higher
- Maven 3.6 or higher
- Google Cloud Platform account with:
  - Pub/Sub subscription created
  - GCS bucket created
  - Appropriate IAM permissions

## Building the Project

```bash
mvn clean package
```

This will create a shaded JAR file at `target/pubsub-to-gcs-beam.jar`.

## Running the Pipeline

### Local Execution (for testing)

```bash
mvn exec:java -Dexec.mainClass="com.example.pubsubtogcs.PubsubToGCSPipeline" \
  -Dexec.args="--inputSubscription=projects/YOUR_PROJECT/subscriptions/YOUR_SUBSCRIPTION \
               --outputBucket=gs://YOUR_BUCKET \
               --runner=DirectRunner"
```

### Dataflow Execution (production)

```bash
java -cp target/pubsub-to-gcs-beam.jar com.example.pubsubtogcs.PubsubToGCSPipeline \
  --inputSubscription=projects/YOUR_PROJECT/subscriptions/YOUR_SUBSCRIPTION \
  --outputBucket=gs://YOUR_BUCKET \
  --runner=DataflowRunner \
  --project=YOUR_PROJECT \
  --region=us-central1 \
  --tempLocation=gs://YOUR_BUCKET/temp \
  --stagingLocation=gs://YOUR_BUCKET/staging
```

## Pipeline Options

- `--inputSubscription`: Pub/Sub subscription to read from (required)
- `--outputBucket`: GCS bucket to write output files (required)
- `--outputPathPrefix`: Optional prefix for output path (default: "output")

## Output Format

### CSV Structure

All output files are in CSV format with the following columns:
- `saga_id`: UUID string
- `node_id`: Base64 or string representation of byte array
- `create_timestamp`: Long timestamp value
- `header`: String representation of byte array
- `body`: String representation of byte array

### File Naming

Files are written with timestamps truncated to minute precision:
- `event_processing/YYYY-MM-DD-HH-MM-*.csv`
- `event_data/YYYY-MM-DD-HH-MM-*.csv`

## Avro Schema

The pipeline expects messages in Avro format with the following schema:

```json
{
  "type": "record",
  "name": "MessageRecord",
  "fields": [
    {"name": "saga_id", "type": ["null", "string"], "default": null},
    {"name": "node_id", "type": ["null", "bytes"], "default": null},
    {"name": "create_timestamp", "type": "long"},
    {"name": "header", "type": ["null", "bytes"], "default": null},
    {"name": "body", "type": ["null", "bytes"], "default": null}
  ]
}
```

## Project Structure

```
pubsubToGCSBeam/
├── pom.xml
├── README.md
├── spec.txt
└── src/
    └── main/
        └── java/
            └── com/
                └── example/
                    └── pubsubtogcs/
                        ├── PubsubToGCSPipeline.java  # Main pipeline class
                        ├── Message.java              # Message model
                        └── AvroMessageParser.java    # Avro parser
```

## Notes

- The pipeline uses 1-minute fixed windows for batching messages
- Files are automatically sharded and written based on window timestamps
- The pipeline handles Avro parsing errors gracefully (logs and continues)
- Byte arrays are converted to strings in the CSV output (you may want to adjust this based on your needs)
