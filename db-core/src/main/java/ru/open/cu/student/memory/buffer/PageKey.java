package ru.open.cu.student.memory.buffer;

import java.util.Objects;


public record PageKey(String fileId, int pageId) {
    public PageKey {
        Objects.requireNonNull(fileId, "fileId");
        if (fileId.isBlank()) {
            throw new IllegalArgumentException("fileId is blank");
        }
        if (pageId < 0) {
            throw new IllegalArgumentException("pageId < 0");
        }
    }
}


