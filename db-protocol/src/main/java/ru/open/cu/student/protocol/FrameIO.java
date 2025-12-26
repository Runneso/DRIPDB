package ru.open.cu.student.protocol;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;


public final class FrameIO {
    
    private static final int MAX_FRAME_BYTES = 16 * 1024 * 1024; 

    private FrameIO() {
    }

    
    public static byte[] readFrame(InputStream in) throws IOException {
        DataInputStream din = (in instanceof DataInputStream d) ? d : new DataInputStream(in);
        final int len;
        try {
            len = din.readInt();
        } catch (EOFException eof) {
            return null;
        }
        if (len < 0 || len > MAX_FRAME_BYTES) {
            throw new IOException("Invalid frame length: " + len);
        }
        byte[] payload = new byte[len];
        din.readFully(payload);
        return payload;
    }

    public static void writeFrame(OutputStream out, byte[] payload) throws IOException {
        if (payload == null) throw new IllegalArgumentException("payload is null");
        if (payload.length > MAX_FRAME_BYTES) {
            throw new IOException("Payload too large: " + payload.length);
        }
        DataOutputStream dout = (out instanceof DataOutputStream d) ? d : new DataOutputStream(out);
        dout.writeInt(payload.length);
        dout.write(payload);
        dout.flush();
    }
}


