package ru.open.cu.student.execution.executors;

import ru.open.cu.student.catalog.manager.CatalogManager;
import ru.open.cu.student.catalog.model.ColumnDefinition;
import ru.open.cu.student.execution.Executor;
import ru.open.cu.student.sql.semantic.CreateTableQueryTree;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public final class CreateTableExecutor implements Executor {
    private final CatalogManager catalog;
    private final CreateTableQueryTree query;

    private boolean executed;

    public CreateTableExecutor(CatalogManager catalog, CreateTableQueryTree query) {
        this.catalog = Objects.requireNonNull(catalog, "catalog");
        this.query = Objects.requireNonNull(query, "query");
    }

    @Override
    public void open() {
        
    }

    @Override
    public List<Object> next() {
        if (executed) return null;
        executed = true;

        List<ColumnDefinition> columns = new ArrayList<>();
        int pos = 0;
        for (CreateTableQueryTree.ResolvedCreateColumn c : query.columns()) {
            columns.add(new ColumnDefinition(
                    c.type().getOid(),
                    c.name().text(),
                    pos++
            ));
        }

        catalog.createTable(query.tableName().text(), columns);
        return null;
    }

    @Override
    public void close() {
        
    }
}


