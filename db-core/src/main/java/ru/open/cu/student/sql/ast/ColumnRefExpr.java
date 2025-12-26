package ru.open.cu.student.sql.ast;

import java.util.Objects;

public record ColumnRefExpr(SqlIdent name) implements Expr {
    public ColumnRefExpr {
        Objects.requireNonNull(name, "name");
    }
}


