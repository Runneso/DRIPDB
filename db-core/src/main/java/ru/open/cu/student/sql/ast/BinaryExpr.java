package ru.open.cu.student.sql.ast;

import java.util.Objects;

public record BinaryExpr(String op, Expr left, Expr right) implements Expr {
    public BinaryExpr {
        Objects.requireNonNull(op, "op");
        Objects.requireNonNull(left, "left");
        Objects.requireNonNull(right, "right");
    }
}


