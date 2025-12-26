package ru.open.cu.student.sql.lexer;


public final class Token {
    private final TokenType type;
    private final String text;
    private final int startOffset;
    private final int endOffset; 
    private final int line;
    private final int column;

    public Token(TokenType type, String text, int startOffset, int endOffset, int line, int column) {
        this.type = type;
        this.text = text;
        this.startOffset = startOffset;
        this.endOffset = endOffset;
        this.line = line;
        this.column = column;
    }

    public TokenType getType() {
        return type;
    }

    public String getText() {
        return text;
    }

    public int getStartOffset() {
        return startOffset;
    }

    public int getEndOffset() {
        return endOffset;
    }

    public int getLine() {
        return line;
    }

    public int getColumn() {
        return column;
    }

    @Override
    public String toString() {
        return type + "(" + text + ")@" + line + ":" + column;
    }
}


