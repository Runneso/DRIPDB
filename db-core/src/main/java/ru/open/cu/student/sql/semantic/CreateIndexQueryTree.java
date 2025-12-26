package ru.open.cu.student.sql.semantic;

import ru.open.cu.student.catalog.model.ColumnDefinition;
import ru.open.cu.student.catalog.model.TableDefinition;
import ru.open.cu.student.index.IndexType;
import ru.open.cu.student.sql.ast.SqlIdent;

import java.util.Objects;

public record CreateIndexQueryTree(
        SqlIdent indexName,
        TableDefinition table,
        ColumnDefinition column,
        IndexType indexType
) implements QueryTree {
    public CreateIndexQueryTree {
        Objects.requireNonNull(indexName, "indexName");
        Objects.requireNonNull(table, "table");
        Objects.requireNonNull(column, "column");
        Objects.requireNonNull(indexType, "indexType");
    }

    @Override
    public QueryType getType() {
        return QueryType.CREATE_INDEX;
    }
}


