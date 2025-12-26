package ru.open.cu.student.cli;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.open.cu.student.protocol.DbErrorPos;
import ru.open.cu.student.protocol.DbRequest;
import ru.open.cu.student.protocol.DbResponse;
import ru.open.cu.student.protocol.FrameIO;
import ru.open.cu.student.protocol.JsonCodec;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public final class DbCliMain {
    private static final Logger log = LoggerFactory.getLogger(DbCliMain.class);

    public static void main(String[] args) {
        String host = "127.0.0.1";
        int port = 54321;
        boolean trace = false;
        String file = null;

        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--host" -> host = args[++i];
                case "--port" -> port = Integer.parseInt(args[++i]);
                case "--trace" -> trace = true;
                case "--file" -> file = args[++i];
                default -> {
                    System.err.println("Unknown arg: " + args[i]);
                    System.err.println("Usage: --host <host> --port <port> [--trace] [--file <path>]");
                    System.exit(2);
                }
            }
        }

        try (Socket socket = new Socket(host, port)) {
            log.info("Connected to {}:{}", host, port);
            System.out.println("Connected. End SQL statements with ';'. Type \\help for help, \\q to quit.");

            if (file != null) {
                runFile(socket, Path.of(file), trace);
                return;
            }

            repl(socket, trace);
        } catch (IOException e) {
            System.err.println("Failed to connect or communicate with server: " + e.getMessage());
            System.exit(1);
        }
    }

    private static void runFile(Socket socket, Path path, boolean trace) throws IOException {
        String content = Files.readString(path, StandardCharsets.UTF_8);
        SplitResult split = splitStatementsWithRemainder(content);
        List<String> statements = split.statements;
        if (!split.remainder.trim().isEmpty()) {
            System.err.println("Warning: trailing SQL without ';' at end of file will be ignored");
        }
        for (String sql : statements) {
            if (sql.isBlank()) continue;
            System.out.println(">>> " + oneLine(sql));
            DbResponse resp = sendQuery(socket, sql, trace);
            printResponse(resp);
        }
    }

    private static void repl(Socket socket, boolean trace) throws IOException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8));
        StringBuilder buf = new StringBuilder();

        while (true) {
            System.out.print(buf.isEmpty() ? "db> " : "   > ");
            String line = reader.readLine();
            if (line == null) {
                System.out.println();
                return;
            }

            String trimmed = line.trim();
            if (buf.isEmpty()) {
                if (trimmed.equalsIgnoreCase("\\q") || trimmed.equalsIgnoreCase("quit") || trimmed.equalsIgnoreCase("exit")) {
                    return;
                }
                if (trimmed.equalsIgnoreCase("\\help")) {
                    printHelp();
                    continue;
                }
            }

            buf.append(line).append('\n');

            SplitResult split = splitStatementsWithRemainder(buf.toString());
            if (split.statements.isEmpty()) continue;

            for (String sql : split.statements) {
                if (sql.isBlank()) continue;
                DbResponse resp = sendQuery(socket, sql, trace);
                printResponse(resp);
            }

            buf.setLength(0);
            buf.append(split.remainder);
        }
    }

    private static void printHelp() {
        System.out.println("Commands:");
        System.out.println("  \\q | quit | exit   - exit");
        System.out.println("  \\help              - help");
        System.out.println();
        System.out.println("End SQL statements with ';'.");
    }

    private static DbResponse sendQuery(Socket socket, String sql, boolean trace) throws IOException {
        String requestId = UUID.randomUUID().toString();
        DbRequest req = new DbRequest("query", requestId, sql, trace);
        byte[] payload = JsonCodec.toJsonBytes(req);
        FrameIO.writeFrame(socket.getOutputStream(), payload);

        byte[] respFrame = FrameIO.readFrame(socket.getInputStream());
        if (respFrame == null) {
            throw new IOException("Server closed connection");
        }
        return JsonCodec.fromJsonBytes(respFrame, DbResponse.class);
    }

    private static void printResponse(DbResponse resp) {
        if (resp == null) {
            System.out.println("(null response)");
            return;
        }

        if (!"ok".equalsIgnoreCase(resp.status)) {
            if (resp.error == null) {
                System.out.println("ERROR: unknown");
                return;
            }
            DbErrorPos pos = resp.error.pos;
            String where = (pos == null) ? "" : (" at " + pos.line + ":" + pos.column);
            System.out.println("ERROR[" + resp.error.code + "]" + where + ": " + resp.error.message);
            return;
        }

        if (resp.explain != null && !resp.explain.isBlank()) {
            System.out.println(resp.explain);
        }

        if (resp.columns != null && resp.rows != null && !resp.columns.isEmpty()) {
            printTable(resp.columns, resp.rows);
        } else if (resp.affected != null) {
            System.out.println("OK (affected=" + resp.affected + ")");
        } else {
            System.out.println("OK");
        }
    }

    private static void printTable(List<String> columns, List<List<Object>> rows) {
        int n = columns.size();
        int[] widths = new int[n];
        for (int i = 0; i < n; i++) {
            widths[i] = columns.get(i) == null ? 4 : columns.get(i).length();
        }
        for (List<Object> row : rows) {
            for (int i = 0; i < n; i++) {
                String s = (row != null && i < row.size()) ? String.valueOf(row.get(i)) : "null";
                widths[i] = Math.max(widths[i], s.length());
            }
        }

        String sep = buildSeparator(widths);
        System.out.println(sep);
        System.out.println(buildRow(columns, widths));
        System.out.println(sep);
        for (List<Object> row : rows) {
            List<String> cells = new ArrayList<>(n);
            for (int i = 0; i < n; i++) {
                Object v = (row != null && i < row.size()) ? row.get(i) : null;
                cells.add(String.valueOf(v));
            }
            System.out.println(buildRow(cells, widths));
        }
        System.out.println(sep);
        System.out.println(rows.size() + " row(s)");
    }

    private static String buildSeparator(int[] widths) {
        StringBuilder sb = new StringBuilder();
        sb.append('+');
        for (int w : widths) {
            sb.append("-".repeat(w + 2)).append('+');
        }
        return sb.toString();
    }

    private static String buildRow(List<?> cells, int[] widths) {
        StringBuilder sb = new StringBuilder();
        sb.append('|');
        for (int i = 0; i < widths.length; i++) {
            String s = (cells != null && i < cells.size() && cells.get(i) != null) ? String.valueOf(cells.get(i)) : "null";
            sb.append(' ').append(padRight(s, widths[i])).append(' ').append('|');
        }
        return sb.toString();
    }

    private static String padRight(String s, int width) {
        if (s.length() >= width) return s;
        return s + " ".repeat(width - s.length());
    }

    private static String oneLine(String sql) {
        return sql.replace('\n', ' ').replace('\r', ' ').trim();
    }

    private record SplitResult(List<String> statements, String remainder) {
    }

    
    private static SplitResult splitStatementsWithRemainder(String text) {
        List<String> out = new ArrayList<>();
        if (text == null || text.isEmpty()) return new SplitResult(out, "");

        boolean inString = false;
        int start = 0;
        for (int i = 0; i < text.length(); i++) {
            char c = text.charAt(i);
            if (c == '\'') {
                if (inString && i + 1 < text.length() && text.charAt(i + 1) == '\'') {
                    i++;
                    continue;
                }
                inString = !inString;
                continue;
            }
            if (c == ';' && !inString) {
                String stmt = text.substring(start, i + 1).trim();
                if (!stmt.isBlank()) {
                    out.add(stmt);
                }
                start = i + 1;
            }
        }
        String remainder = text.substring(start);
        return new SplitResult(out, remainder);
    }
}


