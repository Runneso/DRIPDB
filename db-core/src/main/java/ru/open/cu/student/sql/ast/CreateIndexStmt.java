package ru.open.cu.student.sql.ast;

import ru.open.cu.student.index.IndexType;

import java.util.Objects;

public record CreateIndexStmt(SqlIdent indexName, SqlIdent tableName, SqlIdent columnName, IndexType indexType) implements Statement {
    public CreateIndexStmt {
        Objects.requireNonNull(indexName, "indexName");
        Objects.requireNonNull(tableName, "tableName");
        Objects.requireNonNull(columnName, "columnName");
        Objects.requireNonNull(indexType, "indexType");
    }
}


