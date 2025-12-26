package ru.open.cu.student.server;

import java.nio.file.Path;

public final class DbServerMain {
    public static void main(String[] args) {
        int port = 54321;
        String dataDir = "data";
        int poolSize = 256;
        int flushIntervalMs = 200;
        int flushBatchSize = 64;
        int checkpointIntervalMs = 5_000;

        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--port" -> port = Integer.parseInt(args[++i]);
                case "--dataDir" -> dataDir = args[++i];
                case "--poolSize" -> poolSize = Integer.parseInt(args[++i]);
                case "--flushIntervalMs" -> flushIntervalMs = Integer.parseInt(args[++i]);
                case "--flushBatchSize" -> flushBatchSize = Integer.parseInt(args[++i]);
                case "--checkpointIntervalMs" -> checkpointIntervalMs = Integer.parseInt(args[++i]);
                default -> {
                    System.err.println("Unknown arg: " + args[i]);
                    System.err.println("Usage: --port <port> --dataDir <path> [--poolSize <n>] [--flushIntervalMs <ms>] [--flushBatchSize <n>] [--checkpointIntervalMs <ms>]");
                    System.exit(2);
                }
            }
        }

        DbServer server = new DbServer(port, Path.of(dataDir), poolSize, flushIntervalMs, flushBatchSize, checkpointIntervalMs);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            server.stop();
        }, "db-shutdown"));

        server.start();
    }
}


