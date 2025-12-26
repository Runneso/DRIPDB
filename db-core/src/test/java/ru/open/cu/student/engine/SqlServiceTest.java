package ru.open.cu.student.engine;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import ru.open.cu.student.catalog.manager.DefaultCatalogManager;
import ru.open.cu.student.memory.buffer.BufferPoolManager;
import ru.open.cu.student.memory.buffer.DefaultBufferPoolManager;
import ru.open.cu.student.memory.manager.HeapPageFileManager;
import ru.open.cu.student.memory.replacer.LRUReplacer;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

public class SqlServiceTest {

    @Test
    void create_insert_select_end_to_end(@TempDir Path tempDir) {
        BufferPoolManager bpm = new DefaultBufferPoolManager(16, new HeapPageFileManager(), new LRUReplacer(), tempDir);
        DefaultCatalogManager catalog = new DefaultCatalogManager(tempDir, bpm);

        SqlService svc = new SqlService(tempDir, bpm, catalog);

        ExecutionResult r1 = svc.execute(new SessionContext("s1", "q1", false),
                "CREATE TABLE users (id INT64, name VARCHAR);");
        assertEquals(0, r1.affected());
        assertTrue(r1.rows().isEmpty());

        ExecutionResult r2 = svc.execute(new SessionContext("s1", "q2", false),
                "INSERT INTO users VALUES (42, 'Alice');");
        assertEquals(1, r2.affected());
        assertTrue(r2.rows().isEmpty());

        ExecutionResult r3 = svc.execute(new SessionContext("s1", "q3", false),
                "SELECT name FROM users WHERE id = 42;");
        assertEquals(0, r3.affected());
        assertEquals(java.util.List.of("name"), r3.columns());
        assertEquals(1, r3.rows().size());
        assertEquals(java.util.List.of("Alice"), r3.rows().get(0));
    }

    @Test
    void explain_does_not_execute_inner_statement(@TempDir Path tempDir) {
        BufferPoolManager bpm = new DefaultBufferPoolManager(16, new HeapPageFileManager(), new LRUReplacer(), tempDir);
        DefaultCatalogManager catalog = new DefaultCatalogManager(tempDir, bpm);

        SqlService svc = new SqlService(tempDir, bpm, catalog);

        svc.execute(new SessionContext("s1", "q1", false),
                "CREATE TABLE users (id INT64, name VARCHAR);");

        ExecutionResult explain = svc.execute(new SessionContext("s1", "q2", false),
                "EXPLAIN INSERT INTO users VALUES (1, 'x');");
        assertNotNull(explain.explain());
        assertTrue(explain.rows().isEmpty());

        ExecutionResult after = svc.execute(new SessionContext("s1", "q3", false),
                "SELECT * FROM users;");
        assertTrue(after.rows().isEmpty(), "EXPLAIN must not execute INSERT");
    }

    @Test
    void trace_includes_explain_for_regular_queries(@TempDir Path tempDir) {
        BufferPoolManager bpm = new DefaultBufferPoolManager(16, new HeapPageFileManager(), new LRUReplacer(), tempDir);
        DefaultCatalogManager catalog = new DefaultCatalogManager(tempDir, bpm);

        SqlService svc = new SqlService(tempDir, bpm, catalog);
        svc.execute("CREATE TABLE t (id INT64);");

        ExecutionResult r = svc.execute("INSERT INTO t VALUES (1);", true);
        assertNotNull(r.explain());
        assertTrue(r.explain().contains("TOKENS:"));
    }
}


