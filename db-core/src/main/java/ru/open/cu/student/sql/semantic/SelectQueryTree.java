package ru.open.cu.student.sql.semantic;

import ru.open.cu.student.catalog.model.ColumnDefinition;
import ru.open.cu.student.catalog.model.TableDefinition;

import java.util.List;
import java.util.Objects;

public record SelectQueryTree(
        TableDefinition table,
        List<ColumnDefinition> targetColumns,
        ResolvedExpr filter
) implements QueryTree {
    public SelectQueryTree {
        Objects.requireNonNull(table, "table");
        Objects.requireNonNull(targetColumns, "targetColumns");
        
    }

    @Override
    public QueryType getType() {
        return QueryType.SELECT;
    }
}


