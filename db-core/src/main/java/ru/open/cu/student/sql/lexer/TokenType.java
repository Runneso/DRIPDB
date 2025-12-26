package ru.open.cu.student.sql.lexer;

public enum TokenType {
    
    EOF,

    
    IDENT,
    NUMBER,
    STRING,

    
    CREATE,
    TABLE,
    INDEX,
    ON,
    USING,
    HASH,
    BTREE,

    INSERT,
    INTO,
    VALUES,

    SELECT,
    FROM,
    WHERE,

    AND,
    OR,

    EXPLAIN,

    INT64,
    VARCHAR,

    
    LPAREN,
    RPAREN,
    COMMA,
    SEMICOLON,
    ASTERISK,

    EQ,
    NE,
    LT,
    LE,
    GT,
    GE
}


