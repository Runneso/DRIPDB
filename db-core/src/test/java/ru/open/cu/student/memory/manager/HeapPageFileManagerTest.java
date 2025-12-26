package ru.open.cu.student.memory.manager;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import ru.open.cu.student.memory.page.HeapPage;
import ru.open.cu.student.memory.page.Page;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import static org.junit.jupiter.api.Assertions.*;

public class HeapPageFileManagerTest {

    @Test
    void write_then_read_roundTrip(@TempDir Path tempDir) {
        Path path = tempDir.resolve("db.dat");
        PageFileManager fm = new HeapPageFileManager();

        Page p0 = new HeapPage(0);
        p0.write(new byte[]{1, 2, 3});
        p0.write(new byte[]{4});

        fm.write(p0, path);

        Page r0 = fm.read(0, path);
        assertTrue(r0.isValid());
        assertEquals(2, r0.size());
        assertArrayEquals(new byte[]{1, 2, 3}, r0.read(0));
        assertArrayEquals(new byte[]{4}, r0.read(1));
    }

    @Test
    void read_nonExisting_file_throws(@TempDir Path tempDir) {
        Path path = tempDir.resolve("missing.dat");
        PageFileManager fm = new HeapPageFileManager();
        assertThrows(IllegalArgumentException.class, () -> fm.read(0, path));
    }

    @Test
    void read_page_out_of_bounds_throws(@TempDir Path tempDir) {
        Path path = tempDir.resolve("db.dat");
        PageFileManager fm = new HeapPageFileManager();

        Page p0 = new HeapPage(0);
        p0.write(new byte[]{10});
        fm.write(p0, path);

        assertThrows(IllegalArgumentException.class, () -> fm.read(1, path));
    }

    @Test
    void invalid_signature_on_disk_throws(@TempDir Path tempDir) throws IOException {
        Path path = tempDir.resolve("db.dat");
        Files.write(path, new byte[HeapPage.PAGE_SIZE]);

        PageFileManager fm = new HeapPageFileManager();
        assertThrows(IllegalStateException.class, () -> fm.read(0, path));
    }

    @Test
    void negative_pageId_is_rejected_and_file_is_unchanged(@TempDir Path tempDir) throws IOException {
        Path path = tempDir.resolve("db.dat");
        PageFileManager fm = new HeapPageFileManager();

        Page p0 = new HeapPage(0);
        p0.write(new byte[]{1});
        fm.write(p0, path);

        long sizeBefore = Files.size(path);

        Page bad = new HeapPage(-1);
        bad.write(new byte[]{2, 3});
        assertThrows(IllegalStateException.class, () -> fm.write(bad, path));

        long sizeAfter = Files.size(path);
        assertEquals(sizeBefore, sizeAfter, "file size must remain unchanged after failed write");

        assertEquals(HeapPage.PAGE_SIZE, sizeAfter, "there must be exactly one page in the file");
        Page r0 = fm.read(0, path);
        assertEquals(1, r0.size());
        assertArrayEquals(new byte[]{1}, r0.read(0));

        assertThrows(IllegalArgumentException.class, () -> fm.read(1, path));
    }

    @Test
    void write_nonZero_pageId_then_write_page0_and_read_both(@TempDir Path tempDir) {
        PageFileManager fm = new HeapPageFileManager();
        Path path = tempDir.resolve("pages.bin");

        Page p3 = new HeapPage(3);
        p3.write(new byte[]{7, 8, 9});
        fm.write(p3, path);

        assertThrows(IllegalStateException.class, () -> fm.read(0, path));

        Page p0 = new HeapPage(0);
        p0.write(new byte[]{1});
        fm.write(p0, path);

        Page r0 = fm.read(0, path);
        Page r3 = fm.read(3, path);
        assertArrayEquals(new byte[]{1}, r0.read(0));
        assertArrayEquals(new byte[]{7, 8, 9}, r3.read(0));
    }

    @Test
    void overwrite_existing_page_replaces_content(@TempDir Path tempDir) {
        PageFileManager fm = new HeapPageFileManager();
        Path path = tempDir.resolve("db.dat");

        Page p0 = new HeapPage(0);
        p0.write(new byte[]{1, 2, 3});
        fm.write(p0, path);

        Page p0b = new HeapPage(0);
        p0b.write(new byte[]{9});
        fm.write(p0b, path);

        Page r0 = fm.read(0, path);
        assertEquals(1, r0.size());
        assertArrayEquals(new byte[]{9}, r0.read(0));
    }

    @Test
    void partial_page_on_disk_triggers_illegal_argument(@TempDir Path tempDir) throws IOException {
        PageFileManager fm = new HeapPageFileManager();
        Path path = tempDir.resolve("partial.dat");

        Page p0 = new HeapPage(0);
        p0.write(new byte[]{1});
        fm.write(p0, path);

        long newSize = HeapPage.PAGE_SIZE + HeapPage.PAGE_SIZE / 2L;
        Files.newByteChannel(path, java.util.Set.of(StandardOpenOption.WRITE)).truncate(newSize).close();

        assertThrows(IllegalArgumentException.class, () -> fm.read(1, path));
    }

    @Test
    void read_negative_pageId_is_rejected(@TempDir Path tempDir) {
        PageFileManager fm = new HeapPageFileManager();
        Path path = tempDir.resolve("db.dat");

        Page p0 = new HeapPage(0);
        p0.write(new byte[]{1});
        fm.write(p0, path);

        assertThrows(IllegalStateException.class, () -> fm.read(-1, path));
    }
}


