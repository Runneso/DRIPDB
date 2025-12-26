package ru.open.cu.student.plan;

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
import ru.open.cu.student.optimizer.Optimizer;
import ru.open.cu.student.optimizer.OptimizerImpl;
import ru.open.cu.student.optimizer.node.*;
import ru.open.cu.student.planner.Planner;
import ru.open.cu.student.planner.PlannerImpl;
import ru.open.cu.student.planner.node.LogicalPlanNode;
import ru.open.cu.student.sql.lexer.SqlLexer;
import ru.open.cu.student.sql.parser.SqlParser;
import ru.open.cu.student.sql.semantic.QueryTree;
import ru.open.cu.student.sql.semantic.SqlSemanticAnalyzer;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class PlannerOptimizerTest {

    private static ru.open.cu.student.sql.ast.Statement parse(String sql) {
        return new SqlParser().parse(new SqlLexer().tokenize(sql));
    }

    @Test
    void selects_seqscan_when_no_index(@TempDir Path tempDir) {
        DefaultCatalogManager catalog = newCatalog(tempDir);
        createUsersTable(catalog);

        QueryTree qt = new SqlSemanticAnalyzer().analyze(parse("SELECT * FROM users WHERE id = 42;"), catalog);
        Planner planner = new PlannerImpl();
        LogicalPlanNode logical = planner.plan(qt);

        Optimizer optimizer = new OptimizerImpl(catalog);
        PhysicalPlanNode physical = optimizer.optimize(logical);

        assertInstanceOf(PhysicalProjectNode.class, physical);
        PhysicalPlanNode child = ((PhysicalProjectNode) physical).child();
        assertInstanceOf(PhysicalFilterNode.class, child);
        PhysicalPlanNode scan = ((PhysicalFilterNode) child).child();
        assertInstanceOf(PhysicalSeqScanNode.class, scan);
    }

    @Test
    void selects_hash_index_for_equality(@TempDir Path tempDir) {
        DefaultCatalogManager catalog = newCatalog(tempDir);
        createUsersTable(catalog);
        catalog.createIndex("idx_users_id", "users", "id", IndexType.HASH);

        QueryTree qt = new SqlSemanticAnalyzer().analyze(parse("SELECT * FROM users WHERE id = 42;"), catalog);
        Planner planner = new PlannerImpl();
        LogicalPlanNode logical = planner.plan(qt);

        Optimizer optimizer = new OptimizerImpl(catalog);
        PhysicalPlanNode physical = optimizer.optimize(logical);

        assertInstanceOf(PhysicalProjectNode.class, physical);
        PhysicalPlanNode child = ((PhysicalProjectNode) physical).child();
        assertInstanceOf(PhysicalFilterNode.class, child);
        PhysicalPlanNode scan = ((PhysicalFilterNode) child).child();
        assertInstanceOf(PhysicalHashIndexScanNode.class, scan);
    }

    @Test
    void selects_btree_index_for_range(@TempDir Path tempDir) {
        DefaultCatalogManager catalog = newCatalog(tempDir);
        createUsersTable(catalog);
        catalog.createIndex("idx_users_id_b", "users", "id", IndexType.BTREE);

        QueryTree qt = new SqlSemanticAnalyzer().analyze(parse("SELECT * FROM users WHERE id >= 10 AND id <= 20;"), catalog);
        Planner planner = new PlannerImpl();
        LogicalPlanNode logical = planner.plan(qt);

        Optimizer optimizer = new OptimizerImpl(catalog);
        PhysicalPlanNode physical = optimizer.optimize(logical);

        assertInstanceOf(PhysicalProjectNode.class, physical);
        PhysicalPlanNode child = ((PhysicalProjectNode) physical).child();
        assertInstanceOf(PhysicalFilterNode.class, child);
        PhysicalPlanNode scan = ((PhysicalFilterNode) child).child();
        assertInstanceOf(PhysicalBTreeIndexScanNode.class, scan);
    }

    @Test
    void explain_wraps_plan(@TempDir Path tempDir) {
        DefaultCatalogManager catalog = newCatalog(tempDir);
        createUsersTable(catalog);
        catalog.createIndex("idx_users_id", "users", "id", IndexType.HASH);

        QueryTree qt = new SqlSemanticAnalyzer().analyze(parse("EXPLAIN SELECT * FROM users WHERE id = 1;"), catalog);
        Planner planner = new PlannerImpl();
        LogicalPlanNode logical = planner.plan(qt);

        Optimizer optimizer = new OptimizerImpl(catalog);
        PhysicalPlanNode physical = optimizer.optimize(logical);

        assertInstanceOf(PhysicalExplainNode.class, physical);
    }

    private static DefaultCatalogManager newCatalog(Path root) {
        BufferPoolManager bpm = new DefaultBufferPoolManager(16, new HeapPageFileManager(), new LRUReplacer(), root);
        return new DefaultCatalogManager(root, bpm);
    }

    private static void createUsersTable(DefaultCatalogManager catalog) {
        TypeDefinition int64 = catalog.getTypeByName("INT64");
        TypeDefinition varchar = catalog.getTypeByName("VARCHAR");

        catalog.createTable(
                "users",
                List.of(
                        new ColumnDefinition(int64.getOid(), "id", 0),
                        new ColumnDefinition(varchar.getOid(), "name", 1)
                )
        );
    }
}


