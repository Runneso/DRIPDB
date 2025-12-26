package ru.open.cu.student.sql.lexer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class SqlLexer {
    private static final Map<String, TokenType> KEYWORDS = new HashMap<>();

    static {
        KEYWORDS.put("CREATE", TokenType.CREATE);
        KEYWORDS.put("TABLE", TokenType.TABLE);
        KEYWORDS.put("INDEX", TokenType.INDEX);
        KEYWORDS.put("ON", TokenType.ON);
        KEYWORDS.put("USING", TokenType.USING);
        KEYWORDS.put("HASH", TokenType.HASH);
        KEYWORDS.put("BTREE", TokenType.BTREE);

        KEYWORDS.put("INSERT", TokenType.INSERT);
        KEYWORDS.put("INTO", TokenType.INTO);
        KEYWORDS.put("VALUES", TokenType.VALUES);

        KEYWORDS.put("SELECT", TokenType.SELECT);
        KEYWORDS.put("FROM", TokenType.FROM);
        KEYWORDS.put("WHERE", TokenType.WHERE);

        KEYWORDS.put("AND", TokenType.AND);
        KEYWORDS.put("OR", TokenType.OR);

        KEYWORDS.put("EXPLAIN", TokenType.EXPLAIN);

        KEYWORDS.put("INT64", TokenType.INT64);
        KEYWORDS.put("VARCHAR", TokenType.VARCHAR);
    }

    private String input;
    private int pos;
    private int line;
    private int col;

    public List<Token> tokenize(String sql) {
        if (sql == null) throw new IllegalArgumentException("sql is null");
        this.input = sql;
        this.pos = 0;
        this.line = 1;
        this.col = 1;

        List<Token> out = new ArrayList<>();
        while (!eof()) {
            char c = peek();

            if (isWhitespace(c)) {
                consumeWhitespace();
                continue;
            }

            if (isIdentStart(c)) {
                out.add(readIdentOrKeyword());
                continue;
            }

            if (isDigit(c) || (c == '-' && isDigit(peekNext()))) {
                out.add(readNumber());
                continue;
            }

            if (c == '\'') {
                out.add(readString());
                continue;
            }

            int startPos = pos;
            int startLine = line;
            int startCol = col;

            switch (c) {
                case '(' -> {
                    advance();
                    out.add(new Token(TokenType.LPAREN, "(", startPos, pos, startLine, startCol));
                }
                case ')' -> {
                    advance();
                    out.add(new Token(TokenType.RPAREN, ")", startPos, pos, startLine, startCol));
                }
                case ',' -> {
                    advance();
                    out.add(new Token(TokenType.COMMA, ",", startPos, pos, startLine, startCol));
                }
                case ';' -> {
                    advance();
                    out.add(new Token(TokenType.SEMICOLON, ";", startPos, pos, startLine, startCol));
                }
                case '*' -> {
                    advance();
                    out.add(new Token(TokenType.ASTERISK, "*", startPos, pos, startLine, startCol));
                }
                case '=' -> {
                    advance();
                    out.add(new Token(TokenType.EQ, "=", startPos, pos, startLine, startCol));
                }
                case '<' -> {
                    advance();
                    if (peek() == '=') {
                        advance();
                        out.add(new Token(TokenType.LE, "<=", startPos, pos, startLine, startCol));
                    } else if (peek() == '>') {
                        advance();
                        out.add(new Token(TokenType.NE, "<>", startPos, pos, startLine, startCol));
                    } else {
                        out.add(new Token(TokenType.LT, "<", startPos, pos, startLine, startCol));
                    }
                }
                case '>' -> {
                    advance();
                    if (peek() == '=') {
                        advance();
                        out.add(new Token(TokenType.GE, ">=", startPos, pos, startLine, startCol));
                    } else {
                        out.add(new Token(TokenType.GT, ">", startPos, pos, startLine, startCol));
                    }
                }
                case '!' -> {
                    advance();
                    if (peek() == '=') {
                        advance();
                        out.add(new Token(TokenType.NE, "!=", startPos, pos, startLine, startCol));
                    } else {
                        throw error("Unexpected character: '!'", startPos, startLine, startCol);
                    }
                }
                default -> throw error("Unexpected character: '" + c + "'", startPos, startLine, startCol);
            }
        }

        out.add(new Token(TokenType.EOF, "", pos, pos, line, col));
        return out;
    }

    private Token readIdentOrKeyword() {
        int startPos = pos;
        int startLine = line;
        int startCol = col;

        StringBuilder sb = new StringBuilder();
        while (!eof() && isIdentPart(peek())) {
            sb.append(peek());
            advance();
        }

        String text = sb.toString();
        TokenType kw = KEYWORDS.get(text.toUpperCase());
        TokenType type = kw != null ? kw : TokenType.IDENT;
        return new Token(type, text, startPos, pos, startLine, startCol);
    }

    private Token readNumber() {
        int startPos = pos;
        int startLine = line;
        int startCol = col;

        StringBuilder sb = new StringBuilder();
        if (peek() == '-') {
            sb.append('-');
            advance();
        }
        while (!eof() && isDigit(peek())) {
            sb.append(peek());
            advance();
        }
        return new Token(TokenType.NUMBER, sb.toString(), startPos, pos, startLine, startCol);
    }

    private Token readString() {
        int startPos = pos;
        int startLine = line;
        int startCol = col;

        advance();
        StringBuilder sb = new StringBuilder();

        while (!eof()) {
            char c = peek();
            if (c == '\'') {
                if (peekNext() == '\'') {
                    sb.append('\'');
                    advance();
                    advance();
                    continue;
                }
                break;
            }
            sb.append(c);
            advance();
        }

        if (eof()) {
            throw error("Unterminated string literal", startPos, startLine, startCol);
        }

        advance(); 
        return new Token(TokenType.STRING, sb.toString(), startPos, pos, startLine, startCol);
    }

    private void consumeWhitespace() {
        while (!eof() && isWhitespace(peek())) {
            char c = peek();
            advance();
            if (c == '\n') {
                line++;
                col = 1;
            }
        }
    }

    private boolean eof() {
        return pos >= input.length();
    }

    private char peek() {
        return eof() ? '\0' : input.charAt(pos);
    }

    private char peekNext() {
        return (pos + 1 < input.length()) ? input.charAt(pos + 1) : '\0';
    }

    private void advance() {
        if (eof()) return;
        char c = input.charAt(pos);
        pos++;
        col++;
        if (c == '\n') {
            
        }
    }

    private static boolean isWhitespace(char c) {
        return c == ' ' || c == '\t' || c == '\r' || c == '\n';
    }

    private static boolean isDigit(char c) {
        return c >= '0' && c <= '9';
    }

    private static boolean isIdentStart(char c) {
        return (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || c == '_';
    }

    private static boolean isIdentPart(char c) {
        return isIdentStart(c) || isDigit(c);
    }

    private static SqlSyntaxException error(String message, int offset, int line, int column) {
        return new SqlSyntaxException(message, offset, line, column);
    }
}


