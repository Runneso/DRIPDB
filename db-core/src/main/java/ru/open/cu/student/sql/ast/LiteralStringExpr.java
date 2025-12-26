package ru.open.cu.student.sql.ast;

import java.util.Objects;

public record LiteralStringExpr(String value) implements Expr {
    public LiteralStringExpr {
        Objects.requireNonNull(value, "value");
    }
}


