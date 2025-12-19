package com.example.pubsubtogcs;

import org.junit.Test;
import static org.junit.Assert.*;

import java.util.UUID;

/**
 * Unit tests for Message class.
 */
public class MessageTest {

    @Test
    public void testMessageConstructor() {
        UUID sagaId = UUID.randomUUID();
        byte[] nodeId = "node123".getBytes();
        long timestamp = 1234567890L;
        byte[] header = "header".getBytes();
        byte[] body = "body".getBytes();

        Message message = new Message(sagaId, nodeId, timestamp, header, body);

        assertEquals(sagaId, message.getSagaId());
        assertArrayEquals(nodeId, message.getNodeId());
        assertEquals(timestamp, message.getCreateTimestamp());
        assertArrayEquals(header, message.getHeader());
        assertArrayEquals(body, message.getBody());
    }

    @Test
    public void testMessageSetters() {
        Message message = new Message();
        UUID sagaId = UUID.randomUUID();
        byte[] nodeId = "node".getBytes();
        long timestamp = 9876543210L;
        byte[] header = "h".getBytes();
        byte[] body = "b".getBytes();

        message.setSagaId(sagaId);
        message.setNodeId(nodeId);
        message.setCreateTimestamp(timestamp);
        message.setHeader(header);
        message.setBody(body);

        assertEquals(sagaId, message.getSagaId());
        assertArrayEquals(nodeId, message.getNodeId());
        assertEquals(timestamp, message.getCreateTimestamp());
        assertArrayEquals(header, message.getHeader());
        assertArrayEquals(body, message.getBody());
    }

    @Test
    public void testHasHeaderAndBody_BothPresent() {
        UUID sagaId = UUID.randomUUID();
        byte[] header = "header".getBytes();
        byte[] body = "body".getBytes();
        Message message = new Message(sagaId, null, 1234567890L, header, body);

        assertTrue(message.hasHeaderAndBody());
    }

    @Test
    public void testHasHeaderAndBody_HeaderMissing() {
        UUID sagaId = UUID.randomUUID();
        byte[] body = "body".getBytes();
        Message message = new Message(sagaId, null, 1234567890L, null, body);

        assertFalse(message.hasHeaderAndBody());
    }

    @Test
    public void testHasHeaderAndBody_BodyMissing() {
        UUID sagaId = UUID.randomUUID();
        byte[] header = "header".getBytes();
        Message message = new Message(sagaId, null, 1234567890L, header, null);

        assertFalse(message.hasHeaderAndBody());
    }

    @Test
    public void testHasHeaderAndBody_BothMissing() {
        UUID sagaId = UUID.randomUUID();
        Message message = new Message(sagaId, null, 1234567890L, null, null);

        assertFalse(message.hasHeaderAndBody());
    }

    @Test
    public void testHasHeaderAndBody_EmptyArrays() {
        UUID sagaId = UUID.randomUUID();
        byte[] header = new byte[0];
        byte[] body = new byte[0];
        Message message = new Message(sagaId, null, 1234567890L, header, body);

        assertFalse(message.hasHeaderAndBody());
    }

    @Test
    public void testToCSV_CompleteMessage() {
        UUID sagaId = UUID.fromString("550e8400-e29b-41d4-a716-446655440000");
        byte[] nodeId = "node123".getBytes();
        long timestamp = 1234567890L;
        byte[] header = "header".getBytes();
        byte[] body = "body".getBytes();
        Message message = new Message(sagaId, nodeId, timestamp, header, body);

        String csv = message.toCSV();
        String expected = "550e8400-e29b-41d4-a716-446655440000,node123,1234567890,header,body";
        assertEquals(expected, csv);
    }

    @Test
    public void testToCSV_NullValues() {
        Message message = new Message(null, null, 1234567890L, null, null);

        String csv = message.toCSV();
        String expected = ",,1234567890,,";
        assertEquals(expected, csv);
    }

    @Test
    public void testToCSV_WithCommas() {
        UUID sagaId = UUID.randomUUID();
        byte[] nodeId = "node,with,commas".getBytes();
        byte[] header = "header,test".getBytes();
        byte[] body = "body".getBytes();
        Message message = new Message(sagaId, nodeId, 1234567890L, header, body);

        String csv = message.toCSV();
        assertTrue(csv.contains("\"node,with,commas\""));
        assertTrue(csv.contains("\"header,test\""));
    }

    @Test
    public void testToCSV_WithQuotes() {
        UUID sagaId = UUID.randomUUID();
        byte[] nodeId = "node\"with\"quotes".getBytes();
        byte[] header = "header".getBytes();
        byte[] body = "body".getBytes();
        Message message = new Message(sagaId, nodeId, 1234567890L, header, body);

        String csv = message.toCSV();
        assertTrue(csv.contains("\"node\"\"with\"\"quotes\""));
    }

    @Test
    public void testToCSV_WithNewlines() {
        UUID sagaId = UUID.randomUUID();
        byte[] nodeId = "node\nwith\nnewlines".getBytes();
        byte[] header = "header".getBytes();
        byte[] body = "body".getBytes();
        Message message = new Message(sagaId, nodeId, 1234567890L, header, body);

        String csv = message.toCSV();
        assertTrue(csv.contains("\"node\nwith\nnewlines\""));
    }
}

