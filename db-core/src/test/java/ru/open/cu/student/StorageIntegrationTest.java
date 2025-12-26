package ru.open.cu.student;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import ru.open.cu.student.memory.manager.HeapPageFileManager;
import ru.open.cu.student.memory.manager.PageFileManager;
import ru.open.cu.student.memory.model.DataType;
import ru.open.cu.student.memory.model.HeapTuple;
import ru.open.cu.student.memory.page.HeapPage;
import ru.open.cu.student.memory.page.Page;
import ru.open.cu.student.memory.serializer.HeapTupleSerializer;
import ru.open.cu.student.memory.serializer.TupleSerializer;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

public class StorageIntegrationTest {

    @Test
    void endToEnd_writeReadPagesAndAppend(@TempDir Path tempDir) {
        Path dbFile = tempDir.resolve("heap_database.dat");

        TupleSerializer serializer = new HeapTupleSerializer();
        PageFileManager fileManager = new HeapPageFileManager();

        
        Page page0 = new HeapPage(0);
        long long1 = 42L;
        String str1 = "Привет, мир"; 

        HeapTuple tLong1 = serializer.serialize(long1, DataType.INT64);
        HeapTuple tStr1 = serializer.serialize(str1, DataType.VARCHAR);

        page0.write(tLong1.data());
        page0.write(tStr1.data());

        
        Page page1 = new HeapPage(1);
        String str2 = "hello-世界";
        long long2 = -1234567890123456789L;

        HeapTuple tStr2 = serializer.serialize(str2, DataType.VARCHAR);
        HeapTuple tLong2 = serializer.serialize(long2, DataType.INT64);

        page1.write(tStr2.data());
        page1.write(tLong2.data());

        
        fileManager.write(page0, dbFile);
        fileManager.write(page1, dbFile);

        
        Page r0 = fileManager.read(0, dbFile);
        Page r1 = fileManager.read(1, dbFile);

        assertTrue(r0.isValid());
        assertTrue(r1.isValid());
        assertEquals(0, r0.getPageId());
        assertEquals(1, r1.getPageId());

        
        assertEquals(2, r0.size());
        Long rLong1 = serializer.deserialize(new HeapTuple(r0.read(0), DataType.INT64));
        String rStr1 = serializer.deserialize(new HeapTuple(r0.read(1), DataType.VARCHAR));
        assertEquals(long1, rLong1);
        assertEquals(str1, rStr1);

        
        assertEquals(2, r1.size());
        String rStr2 = serializer.deserialize(new HeapTuple(r1.read(0), DataType.VARCHAR));
        Long rLong2 = serializer.deserialize(new HeapTuple(r1.read(1), DataType.INT64));
        assertEquals(str2, rStr2);
        assertEquals(long2, rLong2);
    }
}


