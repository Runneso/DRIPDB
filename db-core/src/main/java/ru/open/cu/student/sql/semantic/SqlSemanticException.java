package ru.open.cu.student.sql.semantic;

public class SqlSemanticException extends RuntimeException {
    private final Integer offset;
    private final Integer line;
    private final Integer column;

    public SqlSemanticException(String message, Integer offset, Integer line, Integer column) {
        super(message);
        this.offset = offset;
        this.line = line;
        this.column = column;
    }

    public Integer getOffset() {
        return offset;
    }

    public Integer getLine() {
        return line;
    }

    public Integer getColumn() {
        return column;
    }
}


