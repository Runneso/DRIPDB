package ru.open.cu.student.engine;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import ru.open.cu.student.catalog.manager.DefaultCatalogManager;
import ru.open.cu.student.catalog.model.ColumnDefinition;
import ru.open.cu.student.catalog.model.TypeDefinition;
import ru.open.cu.student.index.IndexType;
import ru.open.cu.student.memory.buffer.BufferPoolManager;
import ru.open.cu.student.memory.buffer.DefaultBufferPoolManager;
import ru.open.cu.student.memory.manager.HeapPageFileManager;
import ru.open.cu.student.memory.replacer.LRUReplacer;
import ru.open.cu.student.optimizer.OptimizerImpl;
import ru.open.cu.student.planner.PlannerImpl;
import ru.open.cu.student.sql.lexer.SqlLexer;
import ru.open.cu.student.sql.lexer.Token;
import ru.open.cu.student.sql.parser.SqlParser;
import ru.open.cu.student.sql.semantic.QueryTree;
import ru.open.cu.student.sql.semantic.SqlSemanticAnalyzer;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class ExplainFormatterTest {

    @Test
    void explain_output_contains_all_stages(@TempDir Path tempDir) {
        BufferPoolManager bpm = new DefaultBufferPoolManager(16, new HeapPageFileManager(), new LRUReplacer(), tempDir);
        DefaultCatalogManager catalog = new DefaultCatalogManager(tempDir, bpm);

        TypeDefinition int64 = catalog.getTypeByName("INT64");
        TypeDefinition varchar = catalog.getTypeByName("VARCHAR");
        catalog.createTable("users", List.of(
                new ColumnDefinition(int64.getOid(), "id", 0),
                new ColumnDefinition(varchar.getOid(), "name", 1)
        ));
        catalog.createIndex("idx_users_id", "users", "id", IndexType.HASH);

        String sql = "EXPLAIN SELECT * FROM users WHERE id = 1;";
        SqlLexer lexer = new SqlLexer();
        List<Token> tokens = lexer.tokenize(sql);
        ru.open.cu.student.sql.ast.Statement ast = new SqlParser().parse(tokens);
        QueryTree qt = new SqlSemanticAnalyzer().analyze(ast, catalog);
        var logical = new PlannerImpl().plan(qt);
        var physical = new OptimizerImpl(catalog).optimize(logical);

        String out = ExplainFormatter.format(tokens, ast, qt, logical, physical);
        assertTrue(out.contains("TOKENS:"));
        assertTrue(out.contains("AST:"));
        assertTrue(out.contains("QUERY_TREE:"));
        assertTrue(out.contains("LOGICAL_PLAN:"));
        assertTrue(out.contains("PHYSICAL_PLAN:"));
    }
}


