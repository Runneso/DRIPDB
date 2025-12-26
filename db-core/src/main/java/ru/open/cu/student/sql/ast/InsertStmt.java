package ru.open.cu.student.sql.ast;

import java.util.List;
import java.util.Objects;

public record InsertStmt(SqlIdent tableName, List<Expr> values) implements Statement {
    public InsertStmt {
        Objects.requireNonNull(tableName, "tableName");
        Objects.requireNonNull(values, "values");
    }
}


