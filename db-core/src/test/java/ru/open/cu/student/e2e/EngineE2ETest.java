package ru.open.cu.student.e2e;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import ru.open.cu.student.catalog.manager.DefaultCatalogManager;
import ru.open.cu.student.engine.ExecutionResult;
import ru.open.cu.student.engine.SessionContext;
import ru.open.cu.student.engine.SqlService;
import ru.open.cu.student.memory.buffer.BufferPoolManager;
import ru.open.cu.student.memory.buffer.DefaultBufferPoolManager;
import ru.open.cu.student.memory.manager.HeapPageFileManager;
import ru.open.cu.student.memory.replacer.LRUReplacer;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class EngineE2ETest {

    @Test
    void end_to_end_index_usage_and_persistence_restart(@TempDir Path tempDir) {
        SqlService svc = newService(tempDir, 128);
        SessionContext ctx = new SessionContext("s1", "r1", false);

        svc.execute(ctx, "CREATE TABLE users (id INT64, name VARCHAR);");
        for (int i = 0; i < 100; i++) {
            svc.execute(ctx, "INSERT INTO users VALUES (" + i + ", 'u" + i + "');");
        }

        ExecutionResult explainBefore = svc.execute(ctx, "EXPLAIN SELECT * FROM users WHERE id = 42;");
        assertNotNull(explainBefore.explain());
        assertTrue(explainBefore.explain().contains("SeqScan(users)"), explainBefore.explain());

        svc.execute(ctx, "CREATE INDEX idx_users_id_hash ON users(id) USING HASH;");

        ExecutionResult explainAfterHash = svc.execute(ctx, "EXPLAIN SELECT * FROM users WHERE id = 42;");
        assertNotNull(explainAfterHash.explain());
        assertTrue(explainAfterHash.explain().contains("HashIndexScan(users"), explainAfterHash.explain());

        svc.execute(ctx, "CREATE INDEX idx_users_id_btree ON users(id) USING BTREE;");

        ExecutionResult explainRange = svc.execute(ctx, "EXPLAIN SELECT * FROM users WHERE id >= 10 AND id <= 20;");
        assertNotNull(explainRange.explain());
        assertTrue(explainRange.explain().contains("BTreeIndexScan(users"), explainRange.explain());

        ExecutionResult sel = svc.execute(ctx, "SELECT name FROM users WHERE id = 42;");
        assertEquals(List.of("name"), sel.columns());
        assertEquals(List.of(List.of("u42")), sel.rows());

        
        SqlService svc2 = newService(tempDir, 128);
        SessionContext ctx2 = new SessionContext("s2", "r2", false);

        ExecutionResult sel2 = svc2.execute(ctx2, "SELECT name FROM users WHERE id = 42;");
        assertEquals(List.of("name"), sel2.columns());
        assertEquals(List.of(List.of("u42")), sel2.rows());

        ExecutionResult explainAfterRestart = svc2.execute(ctx2, "EXPLAIN SELECT * FROM users WHERE id = 42;");
        assertNotNull(explainAfterRestart.explain());
        assertTrue(explainAfterRestart.explain().contains("HashIndexScan(users"), explainAfterRestart.explain());
    }

    @Test
    void buffer_pool_small_size_eviction_smoke(@TempDir Path tempDir) {
        SqlService svc = newService(tempDir, 2);
        SessionContext ctx = new SessionContext("s1", "r1", false);

        svc.execute(ctx, "CREATE TABLE t (id INT64, payload VARCHAR);");

        String payload = "x".repeat(200);
        for (int i = 0; i < 200; i++) {
            svc.execute(ctx, "INSERT INTO t VALUES (" + i + ", '" + payload + "');");
        }

        ExecutionResult all = svc.execute(ctx, "SELECT * FROM t;");
        assertEquals(200, all.rows().size());

        
        SqlService svc2 = newService(tempDir, 2);
        ExecutionResult all2 = svc2.execute(new SessionContext("s2", "r2", false), "SELECT * FROM t;");
        assertEquals(200, all2.rows().size());
    }

    private static SqlService newService(Path root, int poolSize) {
        BufferPoolManager bpm = new DefaultBufferPoolManager(poolSize, new HeapPageFileManager(), new LRUReplacer(), root);
        DefaultCatalogManager catalog = new DefaultCatalogManager(root, bpm);
        return new SqlService(root, bpm, catalog);
    }
}


