package ru.open.cu.student.sql.lexer;

public class SqlSyntaxException extends RuntimeException {
    private final int offset;
    private final int line;
    private final int column;

    public SqlSyntaxException(String message, int offset, int line, int column) {
        super(message);
        this.offset = offset;
        this.line = line;
        this.column = column;
    }

    public int getOffset() {
        return offset;
    }

    public int getLine() {
        return line;
    }

    public int getColumn() {
        return column;
    }
}


