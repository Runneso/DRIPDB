package ru.open.cu.student.sql.ast;

import java.util.List;
import java.util.Objects;

public record SelectStmt(boolean selectAll, List<SqlIdent> columns, SqlIdent tableName, Expr where) implements Statement {
    public SelectStmt {
        Objects.requireNonNull(columns, "columns");
        Objects.requireNonNull(tableName, "tableName");
        
    }
}


