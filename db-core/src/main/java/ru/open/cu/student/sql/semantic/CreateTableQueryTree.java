package ru.open.cu.student.sql.semantic;

import ru.open.cu.student.catalog.model.TypeDefinition;
import ru.open.cu.student.sql.ast.SqlIdent;

import java.util.List;
import java.util.Objects;

public record CreateTableQueryTree(SqlIdent tableName, List<ResolvedCreateColumn> columns) implements QueryTree {
    public CreateTableQueryTree {
        Objects.requireNonNull(tableName, "tableName");
        Objects.requireNonNull(columns, "columns");
    }

    @Override
    public QueryType getType() {
        return QueryType.CREATE_TABLE;
    }

    public record ResolvedCreateColumn(SqlIdent name, TypeDefinition type) {
        public ResolvedCreateColumn {
            Objects.requireNonNull(name, "name");
            Objects.requireNonNull(type, "type");
        }
    }
}


