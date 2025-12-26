package ru.open.cu.student.memory.manager;

import ru.open.cu.student.memory.page.HeapPage;
import ru.open.cu.student.memory.page.Page;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public final class HeapPageFileManager implements PageFileManager {
    @Override
    public void write(Page page, Path path) {
        if (page == null) throw new IllegalArgumentException("page is null");
        if (path == null) throw new IllegalArgumentException("path is null");

        byte[] bytes = page.bytes();
        if (bytes == null || bytes.length != HeapPage.PAGE_SIZE) {
            throw new IllegalArgumentException(
                    "page bytes length must be exactly " + HeapPage.PAGE_SIZE +
                            ", got " + (bytes == null ? "null" : bytes.length)
            );
        }

        if (!page.isValid()) {
            throw new IllegalStateException("page has invalid signature");
        }

        int pageId = page.getPageId();
        if (pageId < 0) {
            throw new IllegalStateException("invalid page id: " + pageId);
        }

        try {
            Path parent = path.getParent();
            if (parent != null && !Files.exists(parent)) {
                Files.createDirectories(parent);
            }

            try (FileChannel ch = FileChannel.open(
                    path,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.READ,
                    StandardOpenOption.WRITE
            )) {
                long offset = ((long) pageId) * HeapPage.PAGE_SIZE;
                writeFully(ch, ByteBuffer.wrap(bytes), offset);
            }
        } catch (IOException e) {
            throw new RuntimeException("I/O error while writing page: " + e.getMessage(), e);
        }
    }

    @Override
    public Page read(int pageId, Path path) {
        if (path == null) throw new IllegalArgumentException("path is null");
        if (!Files.exists(path)) throw new IllegalArgumentException("file does not exist: " + path);
        if (pageId < 0) throw new IllegalStateException("invalid page id: " + pageId);

        try (FileChannel ch = FileChannel.open(path, StandardOpenOption.READ)) {
            long fileSize = ch.size();
            long offset = ((long) pageId) * HeapPage.PAGE_SIZE;
            long end = offset + HeapPage.PAGE_SIZE;

            if (end > fileSize) {
                throw new IllegalArgumentException(
                        "page " + pageId + " is out of file bounds (fileSize=" + fileSize + ")"
                );
            }

            byte[] buf = new byte[HeapPage.PAGE_SIZE];
            readFully(ch, ByteBuffer.wrap(buf), offset);

            try {
                return new HeapPage(pageId, buf);
            } catch (IllegalArgumentException badSig) {
                throw new IllegalStateException("invalid page signature at pageId=" + pageId, badSig);
            }
        } catch (IOException e) {
            throw new RuntimeException("I/O error while reading page: " + e.getMessage(), e);
        }
    }

    private static void writeFully(FileChannel channel, ByteBuffer src, long position) throws IOException {
        long pos = position;
        while (src.hasRemaining()) {
            int n = channel.write(src, pos);
            pos += n;
        }
    }

    private static void readFully(FileChannel channel, ByteBuffer dst, long position) throws IOException {
        long pos = position;
        while (dst.hasRemaining()) {
            int n = channel.read(dst, pos);
            if (n < 0) {
                throw new IllegalArgumentException(
                        "read less than " + HeapPage.PAGE_SIZE + " bytes for a page"
                );
            }
            pos += n;
        }
    }
}


