package ru.open.cu.student.sql;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import ru.open.cu.student.catalog.manager.DefaultCatalogManager;
import ru.open.cu.student.catalog.model.ColumnDefinition;
import ru.open.cu.student.catalog.model.TypeDefinition;
import ru.open.cu.student.memory.buffer.BufferPoolManager;
import ru.open.cu.student.memory.buffer.DefaultBufferPoolManager;
import ru.open.cu.student.memory.manager.HeapPageFileManager;
import ru.open.cu.student.memory.replacer.LRUReplacer;
import ru.open.cu.student.sql.lexer.SqlLexer;
import ru.open.cu.student.sql.parser.SqlParser;
import ru.open.cu.student.sql.semantic.*;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class SqlSemanticAnalyzerTest {

    private static SqlSemanticAnalyzer newAnalyzer() {
        return new SqlSemanticAnalyzer();
    }

    private static ru.open.cu.student.sql.ast.Statement parse(String sql) {
        return new SqlParser().parse(new SqlLexer().tokenize(sql));
    }

    @Test
    void select_star_expands_and_where_is_boolean(@TempDir Path tempDir) {
        BufferPoolManager bpm = new DefaultBufferPoolManager(8, new HeapPageFileManager(), new LRUReplacer(), tempDir);
        DefaultCatalogManager catalog = new DefaultCatalogManager(tempDir, bpm);

        TypeDefinition int64 = catalog.getTypeByName("INT64");
        TypeDefinition varchar = catalog.getTypeByName("VARCHAR");

        catalog.createTable(
                "users",
                List.of(
                        new ColumnDefinition(int64.getOid(), "id", 0),
                        new ColumnDefinition(varchar.getOid(), "name", 1)
                )
        );

        QueryTree qt = newAnalyzer().analyze(parse("SELECT * FROM users WHERE id = 1;"), catalog);
        assertInstanceOf(SelectQueryTree.class, qt);
        SelectQueryTree s = (SelectQueryTree) qt;
        assertEquals(2, s.targetColumns().size());
        assertNotNull(s.filter());
        assertEquals(ExprType.BOOL, s.filter().getExprType());
    }

    @Test
    void missing_table_throws_with_position(@TempDir Path tempDir) {
        BufferPoolManager bpm = new DefaultBufferPoolManager(8, new HeapPageFileManager(), new LRUReplacer(), tempDir);
        DefaultCatalogManager catalog = new DefaultCatalogManager(tempDir, bpm);

        SqlSemanticException ex = assertThrows(SqlSemanticException.class,
                () -> newAnalyzer().analyze(parse("SELECT * FROM missing;"), catalog));
        assertNotNull(ex.getOffset());
        assertNotNull(ex.getLine());
        assertNotNull(ex.getColumn());
    }

    @Test
    void missing_column_throws(@TempDir Path tempDir) {
        BufferPoolManager bpm = new DefaultBufferPoolManager(8, new HeapPageFileManager(), new LRUReplacer(), tempDir);
        DefaultCatalogManager catalog = new DefaultCatalogManager(tempDir, bpm);

        TypeDefinition int64 = catalog.getTypeByName("INT64");
        catalog.createTable("t", List.of(new ColumnDefinition(int64.getOid(), "id", 0)));

        assertThrows(SqlSemanticException.class,
                () -> newAnalyzer().analyze(parse("SELECT nope FROM t;"), catalog));
    }

    @Test
    void type_mismatch_in_where_throws(@TempDir Path tempDir) {
        BufferPoolManager bpm = new DefaultBufferPoolManager(8, new HeapPageFileManager(), new LRUReplacer(), tempDir);
        DefaultCatalogManager catalog = new DefaultCatalogManager(tempDir, bpm);

        TypeDefinition int64 = catalog.getTypeByName("INT64");
        catalog.createTable("t", List.of(new ColumnDefinition(int64.getOid(), "id", 0)));

        assertThrows(SqlSemanticException.class,
                () -> newAnalyzer().analyze(parse("SELECT * FROM t WHERE id = 'x';"), catalog));
    }

    @Test
    void create_table_unknown_type_is_semantic_error(@TempDir Path tempDir) {
        BufferPoolManager bpm = new DefaultBufferPoolManager(8, new HeapPageFileManager(), new LRUReplacer(), tempDir);
        DefaultCatalogManager catalog = new DefaultCatalogManager(tempDir, bpm);

        assertThrows(SqlSemanticException.class,
                () -> newAnalyzer().analyze(parse("CREATE TABLE t (a UNKNOWN);"), catalog));
    }

    @Test
    void create_table_int_and_integer_are_accepted_aliases(@TempDir Path tempDir) {
        BufferPoolManager bpm = new DefaultBufferPoolManager(8, new HeapPageFileManager(), new LRUReplacer(), tempDir);
        DefaultCatalogManager catalog = new DefaultCatalogManager(tempDir, bpm);

        QueryTree qt1 = newAnalyzer().analyze(parse("CREATE TABLE t1 (a INT);"), catalog);
        assertInstanceOf(CreateTableQueryTree.class, qt1);

        QueryTree qt2 = newAnalyzer().analyze(parse("CREATE TABLE t2 (a INTEGER);"), catalog);
        assertInstanceOf(CreateTableQueryTree.class, qt2);
    }
}


