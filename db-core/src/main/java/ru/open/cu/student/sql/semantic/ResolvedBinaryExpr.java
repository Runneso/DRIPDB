package ru.open.cu.student.sql.semantic;

import java.util.Objects;

public record ResolvedBinaryExpr(String op, ResolvedExpr left, ResolvedExpr right, ExprType exprType) implements ResolvedExpr {
    public ResolvedBinaryExpr {
        Objects.requireNonNull(op, "op");
        Objects.requireNonNull(left, "left");
        Objects.requireNonNull(right, "right");
        Objects.requireNonNull(exprType, "exprType");
    }

    @Override
    public ExprType getExprType() {
        return exprType;
    }
}


