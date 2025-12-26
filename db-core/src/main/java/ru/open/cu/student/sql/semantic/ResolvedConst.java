package ru.open.cu.student.sql.semantic;

import java.util.Objects;

public record ResolvedConst(Object value, ExprType exprType) implements ResolvedExpr {
    public ResolvedConst {
        Objects.requireNonNull(value, "value");
        Objects.requireNonNull(exprType, "exprType");
    }

    @Override
    public ExprType getExprType() {
        return exprType;
    }
}


