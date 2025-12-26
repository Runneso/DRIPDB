package ru.open.cu.student.index;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import ru.open.cu.student.catalog.manager.DefaultCatalogManager;
import ru.open.cu.student.catalog.model.ColumnDefinition;
import ru.open.cu.student.catalog.model.IndexDefinition;
import ru.open.cu.student.catalog.model.TypeDefinition;
import ru.open.cu.student.memory.buffer.BufferPoolManager;
import ru.open.cu.student.memory.buffer.DefaultBufferPoolManager;
import ru.open.cu.student.memory.manager.HeapPageFileManager;
import ru.open.cu.student.memory.replacer.LRUReplacer;
import ru.open.cu.student.storage.TID;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class DiskHashIndexTest {

    @Test
    void insert_and_search_round_trip_and_persists(@TempDir Path tempDir) {
        BufferPoolManager bpm = new DefaultBufferPoolManager(64, new HeapPageFileManager(), new LRUReplacer(), tempDir);
        DefaultCatalogManager catalog = new DefaultCatalogManager(tempDir, bpm);
        createTable(catalog);

        IndexDefinition def = catalog.createIndex("idx_t_id", "t", "id", IndexType.HASH);
        IndexManager mgr = new IndexManager(tempDir, bpm, catalog);
        Index idx = mgr.getOrCreate(def);

        idx.insert(42L, new TID(0, (short) 1));
        idx.insert(42L, new TID(0, (short) 2));

        List<TID> res = idx.search(42L);
        assertEquals(2, res.size());
        assertTrue(res.contains(new TID(0, (short) 1)));
        assertTrue(res.contains(new TID(0, (short) 2)));

        
        bpm.flushAllPages();

        
        BufferPoolManager bpm2 = new DefaultBufferPoolManager(64, new HeapPageFileManager(), new LRUReplacer(), tempDir);
        DefaultCatalogManager catalog2 = new DefaultCatalogManager(tempDir, bpm2);
        IndexDefinition def2 = catalog2.getIndex("idx_t_id");
        assertNotNull(def2);

        IndexManager mgr2 = new IndexManager(tempDir, bpm2, catalog2);
        Index idx2 = mgr2.getOrCreate(def2);
        List<TID> res2 = idx2.search(42L);
        assertEquals(2, res2.size());
        assertTrue(res2.contains(new TID(0, (short) 1)));
        assertTrue(res2.contains(new TID(0, (short) 2)));
    }

    @Test
    void many_inserts_trigger_splits_and_search_still_works(@TempDir Path tempDir) {
        BufferPoolManager bpm = new DefaultBufferPoolManager(128, new HeapPageFileManager(), new LRUReplacer(), tempDir);
        DefaultCatalogManager catalog = new DefaultCatalogManager(tempDir, bpm);
        createTable(catalog);

        IndexDefinition def = catalog.createIndex("idx_t_id", "t", "id", IndexType.HASH);
        IndexManager mgr = new IndexManager(tempDir, bpm, catalog);
        Index idx = mgr.getOrCreate(def);

        int n = 5_000;
        for (int i = 0; i < n; i++) {
            idx.insert((long) i, new TID(i / 100, (short) (i % 100)));
        }

        DiskHashIndex dh = (DiskHashIndex) idx;
        assertEquals(n, dh.debugRecordCount());
        assertTrue(dh.debugBucketCount() > 16, "Expected splits to increase bucket count");

        int[] probes = {0, 1, 42, 999, 1742, 2048, 4096, 4999};
        for (int k : probes) {
            List<TID> got = idx.search((long) k);
            assertEquals(1, got.size(), "key=" + k);
            assertEquals(new TID(k / 100, (short) (k % 100)), got.get(0), "key=" + k);
        }
    }

    private static void createTable(DefaultCatalogManager catalog) {
        TypeDefinition int64 = catalog.getTypeByName("INT64");
        assertNotNull(int64);
        catalog.createTable("t", List.of(new ColumnDefinition(int64.getOid(), "id", 0)));
    }
}


