package ru.open.cu.student.server;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import ru.open.cu.student.protocol.DbRequest;
import ru.open.cu.student.protocol.DbResponse;
import ru.open.cu.student.protocol.FrameIO;
import ru.open.cu.student.protocol.JsonCodec;

import java.io.DataOutputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

public class DbServerE2ETest {

    @Test
    void single_client_smoke_and_error_handling(@TempDir Path tempDir) throws Exception {
        int port = findFreePort();
        DbServer server = new DbServer(port, tempDir, 128);
        Thread t = new Thread(server::start, "test-db-server");
        t.setDaemon(true);
        t.start();

        waitForServer(port);

        try (Socket sock = new Socket("127.0.0.1", port)) {
            DbResponse r1 = send(sock, "CREATE TABLE users (id INT64, name VARCHAR);", false);
            assertEquals("ok", r1.status);

            DbResponse bad = send(sock, "SEL;", false);
            assertEquals("error", bad.status);
            assertNotNull(bad.error);
            assertEquals("SYNTAX", bad.error.code);

            DbResponse r2 = send(sock, "INSERT INTO users VALUES (1, 'Alice');", false);
            assertEquals("ok", r2.status);

            DbResponse r3 = send(sock, "SELECT name FROM users WHERE id = 1;", false);
            assertEquals("ok", r3.status);
            assertEquals(List.of("name"), r3.columns);
            assertEquals(List.of(List.of("Alice")), r3.rows);
        } finally {
            server.stop();
        }
    }

    @Test
    void framing_partial_write_is_handled(@TempDir Path tempDir) throws Exception {
        int port = findFreePort();
        DbServer server = new DbServer(port, tempDir, 128);
        Thread t = new Thread(server::start, "test-db-server");
        t.setDaemon(true);
        t.start();

        waitForServer(port);

        try (Socket sock = new Socket("127.0.0.1", port)) {
            DbResponse ok = send(sock, "CREATE TABLE t (id INT64);", false);
            assertEquals("ok", ok.status);

            DbRequest req = new DbRequest("query", UUID.randomUUID().toString(), "INSERT INTO t VALUES (1);", false);
            byte[] payload = JsonCodec.toJsonBytes(req);

            OutputStream out = sock.getOutputStream();
            DataOutputStream dout = new DataOutputStream(out);
            dout.writeInt(payload.length);
            dout.flush();

            int mid = payload.length / 2;
            out.write(payload, 0, mid);
            out.flush();
            out.write(payload, mid, payload.length - mid);
            out.flush();

            byte[] respFrame = FrameIO.readFrame(sock.getInputStream());
            assertNotNull(respFrame);
            DbResponse resp = JsonCodec.fromJsonBytes(respFrame, DbResponse.class);
            assertEquals("ok", resp.status);
        } finally {
            server.stop();
        }
    }

    @Test
    void multiple_clients_can_work_in_parallel(@TempDir Path tempDir) throws Exception {
        int port = findFreePort();
        DbServer server = new DbServer(port, tempDir, 256);
        Thread t = new Thread(server::start, "test-db-server");
        t.setDaemon(true);
        t.start();

        waitForServer(port);

        try (Socket setup = new Socket("127.0.0.1", port)) {
            DbResponse r = send(setup, "CREATE TABLE t (id INT64);", false);
            assertEquals("ok", r.status);
        }

        int clients = 5;
        int perClient = 20;
        CountDownLatch latch = new CountDownLatch(clients);
        ConcurrentLinkedQueue<String> errors = new ConcurrentLinkedQueue<>();

        for (int c = 0; c < clients; c++) {
            final int base = c * perClient;
            new Thread(() -> {
                try (Socket sock = new Socket("127.0.0.1", port)) {
                    for (int i = 0; i < perClient; i++) {
                        DbResponse r = send(sock, "INSERT INTO t VALUES (" + (base + i) + ");", false);
                        if (!"ok".equalsIgnoreCase(r.status)) {
                            errors.add("INSERT failed status=" + r.status + " err=" + (r.error == null ? "null" : r.error.code + ":" + r.error.message));
                        }
                    }
                } catch (Exception ignored) {
                    errors.add("client exception: " + ignored);
                } finally {
                    latch.countDown();
                }
            }, "client-" + c).start();
        }

        assertTrue(latch.await(10, TimeUnit.SECONDS), "clients did not finish in time");
        if (!errors.isEmpty()) {
            fail("Client errors:\n" + String.join("\n", errors));
        }

        try (Socket check = new Socket("127.0.0.1", port)) {
            DbResponse resp = send(check, "SELECT * FROM t;", false);
            assertEquals("ok", resp.status);
            assertNotNull(resp.rows);
            assertEquals(clients * perClient, resp.rows.size());
        } finally {
            server.stop();
        }
    }

    private static DbResponse send(Socket sock, String sql, boolean trace) throws Exception {
        DbRequest req = new DbRequest("query", UUID.randomUUID().toString(), sql, trace);
        FrameIO.writeFrame(sock.getOutputStream(), JsonCodec.toJsonBytes(req));
        byte[] resp = FrameIO.readFrame(sock.getInputStream());
        if (resp == null) throw new IllegalStateException("server closed connection");
        return JsonCodec.fromJsonBytes(resp, DbResponse.class);
    }

    private static int findFreePort() throws Exception {
        try (ServerSocket ss = new ServerSocket(0)) {
            return ss.getLocalPort();
        }
    }

    private static void waitForServer(int port) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 5_000;
        while (System.currentTimeMillis() < deadline) {
            try (Socket ignored = new Socket("127.0.0.1", port)) {
                return;
            } catch (Exception e) {
                Thread.sleep(50);
            }
        }
        fail("Server did not start on port " + port);
    }
}


