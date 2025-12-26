package ru.open.cu.student.sql;

import org.junit.jupiter.api.Test;
import ru.open.cu.student.index.IndexType;
import ru.open.cu.student.sql.ast.*;
import ru.open.cu.student.sql.lexer.SqlLexer;
import ru.open.cu.student.sql.lexer.SqlSyntaxException;
import ru.open.cu.student.sql.parser.SqlParser;

import static org.junit.jupiter.api.Assertions.*;

public class SqlParserTest {

    private static Statement parse(String sql) {
        SqlLexer lexer = new SqlLexer();
        SqlParser parser = new SqlParser();
        return parser.parse(lexer.tokenize(sql));
    }

    @Test
    void parses_create_table() {
        Statement s = parse("CREATE TABLE users (id INT64, name VARCHAR);");
        assertInstanceOf(CreateTableStmt.class, s);
        CreateTableStmt ct = (CreateTableStmt) s;
        assertEquals("users", ct.tableName().text());
        assertEquals(2, ct.columns().size());
        assertEquals("id", ct.columns().get(0).name().text());
        assertEquals("INT64", ct.columns().get(0).typeName());
        assertEquals("name", ct.columns().get(1).name().text());
        assertEquals("VARCHAR", ct.columns().get(1).typeName());
    }

    @Test
    void parses_insert() {
        Statement s = parse("INSERT INTO users VALUES (1, 'a');");
        assertInstanceOf(InsertStmt.class, s);
        InsertStmt ins = (InsertStmt) s;
        assertEquals("users", ins.tableName().text());
        assertEquals(2, ins.values().size());
        assertInstanceOf(LiteralInt64Expr.class, ins.values().get(0));
        assertInstanceOf(LiteralStringExpr.class, ins.values().get(1));
    }

    @Test
    void parses_select_where_with_precedence() {
        Statement s = parse("SELECT name FROM users WHERE id >= 10 AND id <= 20;");
        assertInstanceOf(SelectStmt.class, s);
        SelectStmt sel = (SelectStmt) s;
        assertEquals("users", sel.tableName().text());
        assertFalse(sel.selectAll());
        assertEquals(1, sel.columns().size());
        assertEquals("name", sel.columns().get(0).text());

        assertNotNull(sel.where());
        assertInstanceOf(BinaryExpr.class, sel.where());
        BinaryExpr and = (BinaryExpr) sel.where();
        assertEquals("AND", and.op());
    }

    @Test
    void parses_create_index() {
        Statement s = parse("CREATE INDEX idx_users_id ON users(id) USING HASH;");
        assertInstanceOf(CreateIndexStmt.class, s);
        CreateIndexStmt ci = (CreateIndexStmt) s;
        assertEquals("idx_users_id", ci.indexName().text());
        assertEquals("users", ci.tableName().text());
        assertEquals("id", ci.columnName().text());
        assertEquals(IndexType.HASH, ci.indexType());
    }

    @Test
    void parses_explain() {
        Statement s = parse("EXPLAIN SELECT * FROM users;");
        assertInstanceOf(ExplainStmt.class, s);
        ExplainStmt ex = (ExplainStmt) s;
        assertInstanceOf(SelectStmt.class, ex.inner());
    }

    @Test
    void bad_sql_throws_with_position() {
        SqlSyntaxException ex = assertThrows(SqlSyntaxException.class, () -> parse("SELECT FROM users;"));
        assertTrue(ex.getOffset() >= 0);
        assertTrue(ex.getLine() >= 1);
        assertTrue(ex.getColumn() >= 1);
    }
}


