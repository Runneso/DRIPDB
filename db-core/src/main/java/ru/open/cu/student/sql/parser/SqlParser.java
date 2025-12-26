package ru.open.cu.student.sql.parser;

import ru.open.cu.student.index.IndexType;
import ru.open.cu.student.sql.ast.*;
import ru.open.cu.student.sql.lexer.SqlSyntaxException;
import ru.open.cu.student.sql.lexer.Token;
import ru.open.cu.student.sql.lexer.TokenType;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public final class SqlParser {
    private List<Token> tokens;
    private int pos;

    public Statement parse(List<Token> tokens) {
        this.tokens = Objects.requireNonNull(tokens, "tokens");
        this.pos = 0;

        Statement stmt = parseStatement();
        
        expect(TokenType.EOF);
        return stmt;
    }

    private Statement parseStatement() {
        Token t = current();
        return switch (t.getType()) {
            case EXPLAIN -> parseExplain();
            case CREATE -> parseCreate();
            case INSERT -> parseInsert();
            case SELECT -> parseSelect();
            default -> throw error("Unexpected start of statement: " + t.getType(), t);
        };
    }

    private Statement parseExplain() {
        expect(TokenType.EXPLAIN);
        Statement inner = parseStatement();
        match(TokenType.SEMICOLON);
        return new ExplainStmt(inner);
    }

    private Statement parseCreate() {
        expect(TokenType.CREATE);
        Token t = current();
        return switch (t.getType()) {
            case TABLE -> parseCreateTable();
            case INDEX -> parseCreateIndex();
            default -> throw error("Expected TABLE or INDEX after CREATE, got " + t.getType(), t);
        };
    }

    private Statement parseCreateTable() {
        expect(TokenType.TABLE);
        SqlIdent tableName = expectIdent();
        expect(TokenType.LPAREN);

        List<ColumnDef> columns = new ArrayList<>();
        columns.add(parseColumnDef());
        while (match(TokenType.COMMA)) {
            columns.add(parseColumnDef());
        }

        expect(TokenType.RPAREN);
        match(TokenType.SEMICOLON);
        return new CreateTableStmt(tableName, columns);
    }

    private ColumnDef parseColumnDef() {
        SqlIdent name = expectIdent();
        Token t = current();
        String typeName = switch (t.getType()) {
            case INT64, VARCHAR, IDENT -> {
                advance();
                yield t.getText().toUpperCase();
            }
            default -> throw error("Expected type name", t);
        };
        return new ColumnDef(name, typeName);
    }

    private Statement parseCreateIndex() {
        expect(TokenType.INDEX);
        SqlIdent indexName = expectIdent();
        expect(TokenType.ON);
        SqlIdent tableName = expectIdent();
        expect(TokenType.LPAREN);
        SqlIdent columnName = expectIdent();
        expect(TokenType.RPAREN);
        expect(TokenType.USING);

        IndexType type = switch (current().getType()) {
            case HASH -> {
                advance();
                yield IndexType.HASH;
            }
            case BTREE -> {
                advance();
                yield IndexType.BTREE;
            }
            default -> throw error("Expected HASH or BTREE after USING", current());
        };

        match(TokenType.SEMICOLON);
        return new CreateIndexStmt(indexName, tableName, columnName, type);
    }

    private Statement parseInsert() {
        expect(TokenType.INSERT);
        expect(TokenType.INTO);
        SqlIdent tableName = expectIdent();
        expect(TokenType.VALUES);
        expect(TokenType.LPAREN);

        List<Expr> values = new ArrayList<>();
        values.add(parseLiteral());
        while (match(TokenType.COMMA)) {
            values.add(parseLiteral());
        }

        expect(TokenType.RPAREN);
        match(TokenType.SEMICOLON);
        return new InsertStmt(tableName, values);
    }

    private Statement parseSelect() {
        expect(TokenType.SELECT);

        boolean selectAll = false;
        List<SqlIdent> columns = new ArrayList<>();

        if (match(TokenType.ASTERISK)) {
            selectAll = true;
        } else {
            columns.add(expectIdent());
            while (match(TokenType.COMMA)) {
                columns.add(expectIdent());
            }
        }

        expect(TokenType.FROM);
        SqlIdent tableName = expectIdent();

        Expr where = null;
        if (match(TokenType.WHERE)) {
            where = parseExpr();
        }

        match(TokenType.SEMICOLON);
        return new SelectStmt(selectAll, columns, tableName, where);
    }

    
    private Expr parseExpr() {
        return parseOr();
    }

    private Expr parseOr() {
        Expr left = parseAnd();
        while (match(TokenType.OR)) {
            Expr right = parseAnd();
            left = new BinaryExpr("OR", left, right);
        }
        return left;
    }

    private Expr parseAnd() {
        Expr left = parseComparison();
        while (match(TokenType.AND)) {
            Expr right = parseComparison();
            left = new BinaryExpr("AND", left, right);
        }
        return left;
    }

    private Expr parseComparison() {
        Expr left = parsePrimary();
        TokenType t = current().getType();
        String op = switch (t) {
            case EQ -> "=";
            case NE -> "<>";
            case LT -> "<";
            case LE -> "<=";
            case GT -> ">";
            case GE -> ">=";
            default -> null;
        };
        if (op != null) {
            advance();
            Expr right = parsePrimary();
            return new BinaryExpr(op, left, right);
        }
        return left;
    }

    private Expr parsePrimary() {
        Token t = current();
        return switch (t.getType()) {
            case IDENT -> {
                SqlIdent ident = expectIdent();
                yield new ColumnRefExpr(ident);
            }
            case NUMBER, STRING -> parseLiteral();
            case LPAREN -> {
                expect(TokenType.LPAREN);
                Expr e = parseExpr();
                expect(TokenType.RPAREN);
                yield e;
            }
            default -> throw error("Unexpected token in expression: " + t.getType(), t);
        };
    }

    private Expr parseLiteral() {
        Token t = current();
        return switch (t.getType()) {
            case NUMBER -> {
                advance();
                try {
                    yield new LiteralInt64Expr(Long.parseLong(t.getText()));
                } catch (NumberFormatException e) {
                    throw error("Invalid INT64 literal: " + t.getText(), t);
                }
            }
            case STRING -> {
                advance();
                yield new LiteralStringExpr(t.getText());
            }
            default -> throw error("Expected literal (NUMBER or STRING)", t);
        };
    }

    
    private Token current() {
        if (pos >= tokens.size()) return tokens.get(tokens.size() - 1);
        return tokens.get(pos);
    }

    private void advance() {
        if (pos < tokens.size()) pos++;
    }

    private boolean match(TokenType type) {
        if (current().getType() == type) {
            advance();
            return true;
        }
        return false;
    }

    private void expect(TokenType type) {
        Token t = current();
        if (t.getType() != type) {
            throw error("Expected " + type + " but got " + t.getType(), t);
        }
        advance();
    }

    private SqlIdent expectIdent() {
        Token t = current();
        if (t.getType() != TokenType.IDENT) {
            throw error("Expected identifier but got " + t.getType(), t);
        }
        advance();
        return new SqlIdent(t.getText(), t.getStartOffset(), t.getLine(), t.getColumn());
    }

    private static SqlSyntaxException error(String message, Token token) {
        return new SqlSyntaxException(message, token.getStartOffset(), token.getLine(), token.getColumn());
    }
}


