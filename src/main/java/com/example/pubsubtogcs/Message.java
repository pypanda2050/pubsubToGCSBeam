package com.example.pubsubtogcs;

import java.io.Serializable;
import java.util.UUID;

/**
 * Message model representing the PubSub message structure.
 */
public class Message implements Serializable {
    private static final long serialVersionUID = 1L;
    private UUID sagaId;
    private byte[] nodeId;
    private long createTimestamp;
    private byte[] header;
    private byte[] body;

    public Message() {
    }

    public Message(UUID sagaId, byte[] nodeId, long createTimestamp, byte[] header, byte[] body) {
        this.sagaId = sagaId;
        this.nodeId = nodeId;
        this.createTimestamp = createTimestamp;
        this.header = header;
        this.body = body;
    }

    public UUID getSagaId() {
        return sagaId;
    }

    public void setSagaId(UUID sagaId) {
        this.sagaId = sagaId;
    }

    public byte[] getNodeId() {
        return nodeId;
    }

    public void setNodeId(byte[] nodeId) {
        this.nodeId = nodeId;
    }

    public long getCreateTimestamp() {
        return createTimestamp;
    }

    public void setCreateTimestamp(long createTimestamp) {
        this.createTimestamp = createTimestamp;
    }

    public byte[] getHeader() {
        return header;
    }

    public void setHeader(byte[] header) {
        this.header = header;
    }

    public byte[] getBody() {
        return body;
    }

    public void setBody(byte[] body) {
        this.body = body;
    }

    /**
     * Check if both header and body are not empty.
     * @deprecated Use hasHeaderOrBody() instead
     */
    @Deprecated
    public boolean hasHeaderAndBody() {
        return (header != null && header.length > 0) && (body != null && body.length > 0);
    }

    /**
     * Check if either header or body (or both) are not empty.
     */
    public boolean hasHeaderOrBody() {
        return (header != null && header.length > 0) || (body != null && body.length > 0);
    }

    /**
     * Convert message to CSV format.
     */
    public String toCSV() {
        String nodeIdStr = nodeId != null ? new String(nodeId) : "";
        String headerStr = header != null ? new String(header) : "";
        String bodyStr = body != null ? new String(body) : "";
        
        return String.format("%s,%s,%d,%s,%s",
                sagaId != null ? sagaId.toString() : "",
                escapeCSV(nodeIdStr),
                createTimestamp,
                escapeCSV(headerStr),
                escapeCSV(bodyStr));
    }

    /**
     * Escape CSV special characters.
     */
    private String escapeCSV(String value) {
        if (value == null) {
            return "";
        }
        if (value.contains(",") || value.contains("\"") || value.contains("\n")) {
            return "\"" + value.replace("\"", "\"\"") + "\"";
        }
        return value;
    }
}

