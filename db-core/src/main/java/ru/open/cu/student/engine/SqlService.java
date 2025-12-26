package ru.open.cu.student.engine;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.open.cu.student.catalog.manager.CatalogManager;
import ru.open.cu.student.execution.Executor;
import ru.open.cu.student.execution.ExecutorFactory;
import ru.open.cu.student.execution.ExecutorFactoryImpl;
import ru.open.cu.student.execution.QueryExecutionEngine;
import ru.open.cu.student.execution.QueryExecutionEngineImpl;
import ru.open.cu.student.index.IndexManager;
import ru.open.cu.student.memory.buffer.BufferPoolManager;
import ru.open.cu.student.optimizer.Optimizer;
import ru.open.cu.student.optimizer.OptimizerImpl;
import ru.open.cu.student.optimizer.node.PhysicalPlanNode;
import ru.open.cu.student.planner.Planner;
import ru.open.cu.student.planner.PlannerImpl;
import ru.open.cu.student.planner.node.LogicalPlanNode;
import ru.open.cu.student.sql.ast.Statement;
import ru.open.cu.student.sql.lexer.SqlLexer;
import ru.open.cu.student.sql.lexer.Token;
import ru.open.cu.student.sql.parser.SqlParser;
import ru.open.cu.student.sql.semantic.QueryTree;
import ru.open.cu.student.sql.semantic.QueryType;
import ru.open.cu.student.sql.semantic.SelectQueryTree;
import ru.open.cu.student.sql.semantic.SqlSemanticAnalyzer;

import java.nio.file.Path;
import java.util.List;
import java.util.Objects;


public final class SqlService {
    private static final Logger log = LoggerFactory.getLogger(SqlService.class);
    private static final boolean LOG_PIPELINE = Boolean.parseBoolean(System.getProperty("db.logPipeline", "true"));

    private final Path root;
    private final BufferPoolManager bufferPool;
    private final CatalogManager catalog;
    private final IndexManager indexManager;

    private final SqlSemanticAnalyzer semanticAnalyzer = new SqlSemanticAnalyzer();
    private final Planner planner = new PlannerImpl();

    private final Optimizer optimizer;
    private final ExecutorFactory executorFactory;
    private final QueryExecutionEngine engine = new QueryExecutionEngineImpl();

    public SqlService(Path root, BufferPoolManager bufferPool, CatalogManager catalog) {
        this(root, bufferPool, catalog, new IndexManager(root, bufferPool, catalog));
    }

    public SqlService(Path root, BufferPoolManager bufferPool, CatalogManager catalog, IndexManager indexManager) {
        this.root = Objects.requireNonNull(root, "root");
        this.bufferPool = Objects.requireNonNull(bufferPool, "bufferPool");
        this.catalog = Objects.requireNonNull(catalog, "catalog");
        this.indexManager = Objects.requireNonNull(indexManager, "indexManager");

        this.optimizer = new OptimizerImpl(this.catalog);
        this.executorFactory = new ExecutorFactoryImpl(this.root, bufferPool, this.catalog, this.indexManager);
    }

    public ExecutionResult execute(SessionContext ctx, String sql) {
        Objects.requireNonNull(ctx, "ctx");
        return executeInternal(sql, ctx.trace(), ctx.sessionId(), ctx.requestId());
    }

    public ExecutionResult execute(String sql) {
        return executeInternal(sql, false, null, null);
    }

    public ExecutionResult execute(String sql, boolean trace) {
        return executeInternal(sql, trace, null, null);
    }

    private ExecutionResult executeInternal(String sql, boolean trace, String sessionId, String requestId) {
        if (sql == null) throw new IllegalArgumentException("sql is null");

        long startNs = System.nanoTime();

        
        SqlLexer lexer = new SqlLexer();
        SqlParser parser = new SqlParser();
        List<Token> tokens = lexer.tokenize(sql);
        Statement ast = parser.parse(tokens);
        QueryTree queryTree = semanticAnalyzer.analyze(ast, catalog);
        LogicalPlanNode logicalPlan = planner.plan(queryTree);
        PhysicalPlanNode physicalPlan = optimizer.optimize(logicalPlan);

        String pipelineText = null;
        if (LOG_PIPELINE || trace || queryTree.getType() == QueryType.EXPLAIN) {
            pipelineText = ExplainFormatter.format(tokens, ast, queryTree, logicalPlan, physicalPlan);
        }

        if (LOG_PIPELINE && pipelineText != null) {
            log.info(
                    "PIPELINE sessionId={} requestId={} type={}\n{}",
                    sessionId,
                    requestId,
                    queryTree.getType(),
                    pipelineText
            );
        }

        
        if (queryTree.getType() == QueryType.EXPLAIN) {
            long tookMs = (System.nanoTime() - startNs) / 1_000_000;
            log.info("Query done sessionId={} requestId={} type={} tookMs={}", sessionId, requestId, queryTree.getType(), tookMs);
            return new ExecutionResult(List.of(), List.of(), 0, pipelineText);
        }

        Executor executor = executorFactory.createExecutor(physicalPlan);
        List<List<Object>> rows = engine.execute(executor);

        
        if (queryTree.getType() != QueryType.SELECT) {
            bufferPool.flushAllPages();
        }

        List<String> columns = List.of();
        if (queryTree instanceof SelectQueryTree s) {
            columns = s.targetColumns().stream().map(c -> c.getName()).toList();
        }

        int affected = queryTree.getType() == QueryType.INSERT ? 1 : 0;

        long tookMs = (System.nanoTime() - startNs) / 1_000_000;
        log.info("Query done sessionId={} requestId={} type={} tookMs={} rows={} affected={}",
                sessionId, requestId, queryTree.getType(), tookMs, rows.size(), affected);

        String explain = (trace ? pipelineText : null);
        return new ExecutionResult(columns, rows, affected, explain);
    }
}


