package ru.open.cu.student.server;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.open.cu.student.catalog.manager.DefaultCatalogManager;
import ru.open.cu.student.engine.ExecutionResult;
import ru.open.cu.student.engine.SessionContext;
import ru.open.cu.student.engine.SqlService;
import ru.open.cu.student.index.IndexManager;
import ru.open.cu.student.memory.buffer.BufferPoolManager;
import ru.open.cu.student.memory.buffer.DefaultBufferPoolManager;
import ru.open.cu.student.memory.io.DefaultDirtyPageWriter;
import ru.open.cu.student.memory.io.DirtyPageWriter;
import ru.open.cu.student.memory.manager.HeapPageFileManager;
import ru.open.cu.student.memory.replacer.LRUReplacer;
import ru.open.cu.student.protocol.DbError;
import ru.open.cu.student.protocol.DbErrorPos;
import ru.open.cu.student.protocol.DbRequest;
import ru.open.cu.student.protocol.DbResponse;
import ru.open.cu.student.protocol.FrameIO;
import ru.open.cu.student.protocol.JsonCodec;
import ru.open.cu.student.sql.lexer.SqlSyntaxException;
import ru.open.cu.student.sql.semantic.SqlSemanticException;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Path;
import java.util.Objects;
import java.util.UUID;

public final class DbServer {
    private static final Logger log = LoggerFactory.getLogger(DbServer.class);

    private final int port;
    private final Path dataDir;

    private final int bufferPoolSize;
    private final int flushIntervalMs;
    private final int flushBatchSize;
    private final int checkpointIntervalMs;

    private volatile boolean running;
    private ServerSocket serverSocket;

    private final BufferPoolManager bufferPool;
    private final DirtyPageWriter dirtyPageWriter;
    private final SqlService sqlService;

    public DbServer(int port, String dataDir) {
        this(port, Path.of(dataDir), 256);
    }

    public DbServer(int port, Path dataDir, int bufferPoolSize) {
        this(port, dataDir, bufferPoolSize, 200, 64, 5_000);
    }

    public DbServer(int port, Path dataDir, int bufferPoolSize, int flushIntervalMs, int flushBatchSize, int checkpointIntervalMs) {
        if (port <= 0 || port > 65535) {
            throw new IllegalArgumentException("Invalid port: " + port);
        }
        if (bufferPoolSize <= 0) {
            throw new IllegalArgumentException("bufferPoolSize must be > 0");
        }
        this.port = port;
        this.dataDir = Objects.requireNonNull(dataDir, "dataDir");
        this.bufferPoolSize = bufferPoolSize;
        this.flushIntervalMs = flushIntervalMs;
        this.flushBatchSize = flushBatchSize;
        this.checkpointIntervalMs = checkpointIntervalMs;

        this.bufferPool = new DefaultBufferPoolManager(
                bufferPoolSize,
                new HeapPageFileManager(),
                new LRUReplacer(),
                this.dataDir
        );
        DefaultCatalogManager catalog = new DefaultCatalogManager(this.dataDir, this.bufferPool);
        IndexManager indexManager = new IndexManager(this.dataDir, this.bufferPool, catalog);
        this.sqlService = new SqlService(this.dataDir, this.bufferPool, catalog, indexManager);
        this.dirtyPageWriter = new DefaultDirtyPageWriter(this.bufferPool, flushIntervalMs, flushBatchSize, checkpointIntervalMs);
    }

    public void start() {
        running = true;
        try (ServerSocket ss = new ServerSocket(port)) {
            this.serverSocket = ss;
            log.info("DB server started on port={} dataDir={} bufferPoolSize={}", port, dataDir, bufferPoolSize);
            log.info("DirtyPageWriter enabled: flushIntervalMs={} batchSize={} checkpointIntervalMs={}",
                    flushIntervalMs, flushBatchSize, checkpointIntervalMs);

            dirtyPageWriter.startBackgroundWriter();
            dirtyPageWriter.startCheckPointer();

            while (running) {
                Socket client = ss.accept();
                String sessionId = UUID.randomUUID().toString();
                Thread t = new Thread(() -> handleClient(sessionId, client), "db-session-" + sessionId);
                t.setDaemon(true);
                t.start();
            }
        } catch (IOException e) {
            if (running) {
                log.error("Server stopped due to I/O error", e);
            } else {
                log.info("Server stopped");
            }
        } finally {
            running = false;
        }
    }

    public void stop() {
        running = false;
        try {
            bufferPool.flushAllPages();
        } catch (Exception e) {
            log.warn("Failed to flush pages on stop", e);
        }
        try {
            if (serverSocket != null) {
                serverSocket.close();
            }
        } catch (IOException ignored) {
        }
    }

    private void handleClient(String sessionId, Socket socket) {
        String remote = socket.getRemoteSocketAddress() == null ? "unknown" : socket.getRemoteSocketAddress().toString();
        log.info("Session started sessionId={} remote={}", sessionId, remote);

        try (Socket s = socket) {
            while (true) {
                byte[] frame = FrameIO.readFrame(s.getInputStream());
                if (frame == null) {
                    break; 
                }

                DbRequest req;
                try {
                    req = JsonCodec.fromJsonBytes(frame, DbRequest.class);
                } catch (Exception badJson) {
                    DbResponse resp = DbResponse.error(null, new DbError("SYNTAX", "Invalid JSON request", null));
                    FrameIO.writeFrame(s.getOutputStream(), JsonCodec.toJsonBytes(resp));
                    continue;
                }

                DbResponse resp = processRequest(sessionId, req);
                FrameIO.writeFrame(s.getOutputStream(), JsonCodec.toJsonBytes(resp));
            }
        } catch (IOException io) {
            log.info("Session IO closed sessionId={} remote={} err={}", sessionId, remote, io.toString());
        } catch (Throwable t) {
            log.error("Session crashed sessionId={} remote={}", sessionId, remote, t);
        } finally {
            log.info("Session ended sessionId={} remote={}", sessionId, remote);
        }
    }

    private DbResponse processRequest(String sessionId, DbRequest req) {
        String requestId = req == null ? null : req.requestId;
        if (req == null) {
            return DbResponse.error(null, new DbError("EXEC", "Request is null", null));
        }
        if (!"query".equalsIgnoreCase(req.type)) {
            return DbResponse.error(requestId, new DbError("EXEC", "Unsupported request type: " + req.type, null));
        }
        if (req.sql == null) {
            return DbResponse.error(requestId, new DbError("EXEC", "sql is null", null));
        }

        try {
            ExecutionResult r = sqlService.execute(new SessionContext(sessionId, requestId, req.trace), req.sql);
            return DbResponse.ok(requestId, r.columns(), r.rows(), r.affected(), r.explain());
        } catch (SqlSyntaxException e) {
            return DbResponse.error(requestId, new DbError("SYNTAX", e.getMessage(), new DbErrorPos(e.getOffset(), e.getLine(), e.getColumn())));
        } catch (SqlSemanticException e) {
            DbErrorPos pos = (e.getOffset() == null && e.getLine() == null && e.getColumn() == null)
                    ? null
                    : new DbErrorPos(e.getOffset(), e.getLine(), e.getColumn());
            return DbResponse.error(requestId, new DbError("SEMANTIC", e.getMessage(), pos));
        } catch (IllegalArgumentException e) {
            return DbResponse.error(requestId, new DbError("EXEC", e.getMessage(), null));
        } catch (Throwable t) {
            return DbResponse.error(requestId, new DbError("EXEC", t.getClass().getSimpleName() + ": " + t.getMessage(), null));
        }
    }
}


