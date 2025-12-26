package ru.open.cu.student.catalog.manager;

import ru.open.cu.student.catalog.model.ColumnDefinition;
import ru.open.cu.student.catalog.model.IndexDefinition;
import ru.open.cu.student.catalog.model.TableDefinition;
import ru.open.cu.student.catalog.model.TypeDefinition;
import ru.open.cu.student.index.IndexType;

import java.util.List;

public interface CatalogManager {
    TableDefinition createTable(String name, List<ColumnDefinition> columns);

    TableDefinition getTable(String tableName);

    List<TableDefinition> listTables();

    List<ColumnDefinition> getColumns(TableDefinition table);

    ColumnDefinition getColumn(TableDefinition table, String columnName);

    TypeDefinition getTypeByOid(int oid);

    TypeDefinition getTypeByName(String typeName);

    void updatePagesCount(TableDefinition table, int pagesCount);

    IndexDefinition createIndex(String indexName, String tableName, String columnName, IndexType indexType);

    IndexDefinition getIndex(String indexName);

    List<IndexDefinition> listIndexes(TableDefinition table);
}


