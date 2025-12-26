package ru.open.cu.student.memory.io;

import ru.open.cu.student.memory.buffer.BufferPoolManager;
import ru.open.cu.student.memory.model.BufferSlot;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

public final class DefaultDirtyPageWriter implements DirtyPageWriter {
    private static final String BACKGROUND_THREAD_NAME = "DirtyPageWriter-BackgroundWriterThread";
    private static final String CHECKPOINT_THREAD_NAME = "DirtyPageWriter-CheckPointerThread";

    private final BufferPoolManager manager;
    private final int intervalMs;
    private final int checkpointIntervalMs;
    private final int batchSize;

    private final AtomicBoolean backgroundFlag = new AtomicBoolean(false);
    private final AtomicBoolean checkpointFlag = new AtomicBoolean(false);

    private Thread backgroundThread;
    private Thread checkpointThread;

    public DefaultDirtyPageWriter(BufferPoolManager manager, int intervalMs, int batchSize, int checkpointIntervalMs) {
        if (intervalMs <= 0 || checkpointIntervalMs <= 0) {
            throw new IllegalArgumentException("intervalMs and checkpointIntervalMs must be > 0");
        }
        if (batchSize <= 0) {
            throw new IllegalArgumentException("batchSize must be > 0");
        }
        this.manager = Objects.requireNonNull(manager, "manager");
        this.intervalMs = intervalMs;
        this.batchSize = batchSize;
        this.checkpointIntervalMs = checkpointIntervalMs;
    }

    @Override
    public void startBackgroundWriter() {
        if (!backgroundFlag.compareAndSet(false, true)) return;

        backgroundThread = new Thread(() -> {
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    Thread.sleep(intervalMs);
                    flushBatch();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } catch (Throwable ignored) {
                    
                }
            }
        }, BACKGROUND_THREAD_NAME);

        backgroundThread.setDaemon(true);
        backgroundThread.start();
    }

    @Override
    public void startCheckPointer() {
        if (!checkpointFlag.compareAndSet(false, true)) return;

        checkpointThread = new Thread(() -> {
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    Thread.sleep(checkpointIntervalMs);
                    manager.flushAllPages();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } catch (Throwable ignored) {
                    
                }
            }
        }, CHECKPOINT_THREAD_NAME);

        checkpointThread.setDaemon(true);
        checkpointThread.start();
    }

    private void flushBatch() {
        List<BufferSlot> dirty = manager.getDirtyPages();
        int flushed = 0;
        for (BufferSlot s : dirty) {
            if (flushed >= batchSize) break;
            manager.flushPage(s.getKey());
            flushed++;
        }
    }
}


