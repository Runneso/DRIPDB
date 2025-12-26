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
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class DiskBTreeIndexTest {

    @Test
    void insert_and_search_duplicates(@TempDir Path tempDir) {
        BufferPoolManager bpm = new DefaultBufferPoolManager(64, new HeapPageFileManager(), new LRUReplacer(), tempDir);
        DefaultCatalogManager catalog = new DefaultCatalogManager(tempDir, bpm);
        createTable(catalog);

        IndexDefinition def = catalog.createIndex("idx_t_id", "t", "id", IndexType.BTREE);
        IndexManager mgr = new IndexManager(tempDir, bpm, catalog);
        Index idx = mgr.getOrCreate(def);

        idx.insert(10L, new TID(0, (short) 1));
        idx.insert(10L, new TID(0, (short) 2));

        List<TID> res = idx.search(10L);
        assertEquals(2, res.size());
        assertTrue(res.contains(new TID(0, (short) 1)));
        assertTrue(res.contains(new TID(0, (short) 2)));
    }

    @Test
    void rangeSearch_across_many_keys_sorted(@TempDir Path tempDir) {
        BufferPoolManager bpm = new DefaultBufferPoolManager(128, new HeapPageFileManager(), new LRUReplacer(), tempDir);
        DefaultCatalogManager catalog = new DefaultCatalogManager(tempDir, bpm);
        createTable(catalog);

        IndexDefinition def = catalog.createIndex("idx_t_id", "t", "id", IndexType.BTREE);
        IndexManager mgr = new IndexManager(tempDir, bpm, catalog);
        Index idx = mgr.getOrCreate(def);

        for (int i = 0; i < 100; i++) {
            idx.insert((long) i, new TID(0, (short) i));
        }

        List<TID> res = idx.rangeSearch(10L, true, 20L, true);
        assertEquals(11, res.size());
        assertEquals(new TID(0, (short) 10), res.get(0));
        assertEquals(new TID(0, (short) 20), res.get(res.size() - 1));
    }

    @Test
    void scanAll_returns_all_in_sorted_key_order(@TempDir Path tempDir) {
        BufferPoolManager bpm = new DefaultBufferPoolManager(128, new HeapPageFileManager(), new LRUReplacer(), tempDir);
        DefaultCatalogManager catalog = new DefaultCatalogManager(tempDir, bpm);
        createTable(catalog);

        IndexDefinition def = catalog.createIndex("idx_t_id", "t", "id", IndexType.BTREE);
        IndexManager mgr = new IndexManager(tempDir, bpm, catalog);
        Index idx = mgr.getOrCreate(def);

        int[] keys = {5, 1, 9, 3, 7, 2, 8, 6, 4, 0};
        for (int k : keys) {
            idx.insert((long) k, new TID(0, (short) k));
        }

        List<TID> all = idx.rangeSearch(null, true, null, true);
        assertEquals(10, all.size());

        List<Integer> seen = new ArrayList<>();
        for (TID tid : all) {
            seen.add((int) tid.slotId());
        }
        assertEquals(List.of(0, 1, 2, 3, 4, 5, 6, 7, 8, 9), seen);
    }

    @Test
    void splits_increase_height_and_persistence_restart(@TempDir Path tempDir) {
        BufferPoolManager bpm = new DefaultBufferPoolManager(256, new HeapPageFileManager(), new LRUReplacer(), tempDir);
        DefaultCatalogManager catalog = new DefaultCatalogManager(tempDir, bpm);
        createTable(catalog);

        IndexDefinition def = catalog.createIndex("idx_t_id", "t", "id", IndexType.BTREE);
        IndexManager mgr = new IndexManager(tempDir, bpm, catalog);
        Index idx = mgr.getOrCreate(def);

        int n = 1_000;
        for (int i = 0; i < n; i++) {
            idx.insert((long) i, new TID(i / 100, (short) (i % 100)));
        }

        DiskBTreeIndex bt = (DiskBTreeIndex) idx;
        assertTrue(bt.debugHeight() >= 2, "Expected splits to increase tree height");

        int[] probes = {0, 1, 42, 999};
        for (int k : probes) {
            List<TID> got = idx.search((long) k);
            assertEquals(1, got.size());
            assertEquals(new TID(k / 100, (short) (k % 100)), got.get(0));
        }

        
        bpm.flushAllPages();

        BufferPoolManager bpm2 = new DefaultBufferPoolManager(256, new HeapPageFileManager(), new LRUReplacer(), tempDir);
        DefaultCatalogManager catalog2 = new DefaultCatalogManager(tempDir, bpm2);
        IndexDefinition def2 = catalog2.getIndex("idx_t_id");
        assertNotNull(def2);

        IndexManager mgr2 = new IndexManager(tempDir, bpm2, catalog2);
        Index idx2 = mgr2.getOrCreate(def2);

        for (int k : probes) {
            List<TID> got = idx2.search((long) k);
            assertEquals(1, got.size());
            assertEquals(new TID(k / 100, (short) (k % 100)), got.get(0));
        }
    }

    private static void createTable(DefaultCatalogManager catalog) {
        TypeDefinition int64 = catalog.getTypeByName("INT64");
        assertNotNull(int64);
        catalog.createTable("t", List.of(new ColumnDefinition(int64.getOid(), "id", 0)));
    }
}


