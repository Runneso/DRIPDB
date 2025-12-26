package ru.open.cu.student.memory.manager;

import ru.open.cu.student.memory.page.Page;

import java.nio.file.Path;

public interface PageFileManager {
    void write(Page page, Path path);

    Page read(int pageId, Path path);
}


