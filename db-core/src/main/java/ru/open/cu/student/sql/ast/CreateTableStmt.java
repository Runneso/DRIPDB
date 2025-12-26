package ru.open.cu.student.sql.ast;

import java.util.List;
import java.util.Objects;

public record CreateTableStmt(SqlIdent tableName, List<ColumnDef> columns) implements Statement {
    public CreateTableStmt {
        Objects.requireNonNull(tableName, "tableName");
        Objects.requireNonNull(columns, "columns");
    }
}


