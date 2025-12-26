package ru.open.cu.student.sql.semantic;

import ru.open.cu.student.catalog.model.ColumnDefinition;
import ru.open.cu.student.catalog.model.TableDefinition;

import java.util.List;
import java.util.Objects;

public record InsertQueryTree(TableDefinition table, List<ColumnDefinition> columns, List<Object> values) implements QueryTree {
    public InsertQueryTree {
        Objects.requireNonNull(table, "table");
        Objects.requireNonNull(columns, "columns");
        Objects.requireNonNull(values, "values");
    }

    @Override
    public QueryType getType() {
        return QueryType.INSERT;
    }
}


