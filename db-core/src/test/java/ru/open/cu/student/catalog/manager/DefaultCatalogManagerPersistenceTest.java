package ru.open.cu.student.catalog.manager;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import ru.open.cu.student.catalog.model.ColumnDefinition;
import ru.open.cu.student.catalog.model.IndexDefinition;
import ru.open.cu.student.catalog.model.TableDefinition;
import ru.open.cu.student.catalog.model.TypeDefinition;
import ru.open.cu.student.index.IndexType;
import ru.open.cu.student.memory.buffer.BufferPoolManager;
import ru.open.cu.student.memory.buffer.DefaultBufferPoolManager;
import ru.open.cu.student.memory.manager.HeapPageFileManager;
import ru.open.cu.student.memory.manager.PageFileManager;
import ru.open.cu.student.memory.replacer.LRUReplacer;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class DefaultCatalogManagerPersistenceTest {

    @Test
    void restart_loads_tables_columns_types_and_indexes(@TempDir Path tempDir) {
        PageFileManager pfm = new HeapPageFileManager();
        BufferPoolManager bpm = new DefaultBufferPoolManager(8, pfm, new LRUReplacer(), tempDir);

        DefaultCatalogManager catalog = new DefaultCatalogManager(tempDir, bpm);

        TypeDefinition int64 = catalog.getTypeByName("INT64");
        TypeDefinition varchar = catalog.getTypeByName("VARCHAR");
        assertNotNull(int64);
        assertNotNull(varchar);

        TableDefinition users = catalog.createTable(
                "users",
                List.of(
                        new ColumnDefinition(int64.getOid(), "id", 0),
                        new ColumnDefinition(varchar.getOid(), "name", 1)
                )
        );

        IndexDefinition idx = catalog.createIndex("idx_users_id", "users", "id", IndexType.HASH);
        assertNotNull(idx);

        
        BufferPoolManager bpm2 = new DefaultBufferPoolManager(8, new HeapPageFileManager(), new LRUReplacer(), tempDir);
        DefaultCatalogManager catalog2 = new DefaultCatalogManager(tempDir, bpm2);

        TypeDefinition int642 = catalog2.getTypeByName("INT64");
        TypeDefinition varchar2 = catalog2.getTypeByName("VARCHAR");
        assertNotNull(int642);
        assertNotNull(varchar2);

        TableDefinition users2 = catalog2.getTable("users");
        assertNotNull(users2);
        assertEquals(users.getOid(), users2.getOid());
        assertEquals(users.getFileNode(), users2.getFileNode());

        List<ColumnDefinition> cols = catalog2.getColumns(users2);
        assertEquals(2, cols.size());
        assertEquals("id", cols.get(0).getName());
        assertEquals("name", cols.get(1).getName());

        IndexDefinition idx2 = catalog2.getIndex("idx_users_id");
        assertNotNull(idx2);
        assertEquals(IndexType.HASH, idx2.getIndexType());
        assertEquals(users2.getOid(), idx2.getTableOid());
        assertEquals(cols.get(0).getOid(), idx2.getColumnOid());
        assertEquals(cols.get(0).getTypeOid(), idx2.getKeyTypeOid());
    }
}


