package ru.open.cu.student.sql.semantic;

import ru.open.cu.student.catalog.model.ColumnDefinition;

import java.util.Objects;

public record ResolvedColumnRef(ColumnDefinition column, ExprType exprType) implements ResolvedExpr {
    public ResolvedColumnRef {
        Objects.requireNonNull(column, "column");
        Objects.requireNonNull(exprType, "exprType");
    }

    @Override
    public ExprType getExprType() {
        return exprType;
    }
}


