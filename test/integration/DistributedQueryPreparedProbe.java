/* Copyright 2026 Project Tsurugi. Licensed under the Apache License, Version 2.0. */
import com.tsurugidb.sql.proto.SqlRequest.Parameter;
import com.tsurugidb.sql.proto.SqlRequest.TransactionOption;
import com.tsurugidb.sql.proto.SqlRequest.TransactionType;
import com.tsurugidb.tsubakuro.common.SessionBuilder;
import com.tsurugidb.tsubakuro.sql.Parameters;
import com.tsurugidb.tsubakuro.sql.Placeholders;
import com.tsurugidb.tsubakuro.sql.PreparedStatement;
import com.tsurugidb.tsubakuro.sql.SqlClient;
import com.tsurugidb.tsubakuro.sql.SqlServiceCode;
import com.tsurugidb.tsubakuro.sql.SqlServiceException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/** SQL-client probes that retain a prepared statement across catalog/worker DDL. */
class DistributedQueryPreparedProbe {
    private static final TransactionOption RTX = TransactionOption.newBuilder()
            .setType(TransactionType.READ_ONLY).build();

    @FunctionalInterface
    private interface Action { void run() throws Exception; }

    private static void error(SqlServiceCode expected, String detail, Action action) throws Exception {
        try {
            action.run();
        } catch (SqlServiceException e) {
            if (e.getDiagnosticCode() != expected || (detail != null && !e.getMessage().contains(detail))) {
                throw new IllegalStateException("Unexpected SQL error: " + e.getDiagnosticCode(), e);
            }
            System.out.println("EXPECTED " + expected + " " + e.getMessage().replace('\n', ' '));
            return;
        }
        throw new IllegalStateException("Expected SQL error " + expected);
    }

    private static List<Long> read(SqlClient sql, PreparedStatement statement, Parameter... parameters)
            throws Exception {
        try (var tx = sql.createTransaction(RTX).await()) {
            var values = new ArrayList<Long>();
            try (var result = tx.executeQuery(statement, parameters).await()) {
                while (result.nextRow()) {
                    if (!result.nextColumn() || result.isNull()) throw new IllegalStateException("Missing key");
                    values.add(result.fetchInt8Value());
                    if (result.nextColumn()) throw new IllegalStateException("Unexpected column");
                }
            }
            tx.commit().await();
            return values;
        }
    }

    private static void expectRows(List<Long> expected, List<Long> actual) {
        if (!actual.equals(expected)) throw new IllegalStateException("Expected " + expected + ", got " + actual);
    }

    private static void literal(SqlClient sql, String text, List<Long> expected) throws Exception {
        try (var statement = sql.prepare(text).await()) {
            expectRows(expected, read(sql, statement));
        }
    }

    private static void await(Path directory, String event) throws Exception {
        var until = Instant.now().plus(Duration.ofSeconds(40));
        while (!Files.exists(directory.resolve(event))) {
            if (Instant.now().isAfter(until)) throw new IllegalStateException("No test event " + event);
            Thread.sleep(10);
        }
    }

    private static void signal(Path directory, String event) throws Exception {
        Files.writeString(directory.resolve(event), event);
    }

    public static void main(String[] args) throws Exception {
        try (var session = SessionBuilder.connect(args[1]).create(); var sql = SqlClient.attach(session)) {
            String mode = args[0];
            String text = args[2];
            if (mode.equals("bindings")) {
                try (var statement = sql.prepare(text, Placeholders.of("key", long.class)).await()) {
                    for (Long key : Arrays.asList(900L, null, 1000L)) {
                        var expected = key == null ? List.<Long>of() : List.of(key);
                        var parameter = key == null ? Parameters.ofNull("key") : Parameters.of("key", key.longValue());
                        expectRows(expected, read(sql, statement, parameter));
                        literal(sql, text.replace(":key", key == null ? "CAST(NULL AS BIGINT)" : key.toString()), expected);
                        System.out.println("BIND_OK " + key);
                    }
                    error(SqlServiceCode.UNRESOLVED_PLACEHOLDER_EXCEPTION, null, () -> read(sql, statement));
                    expectRows(List.of(900L), read(sql, statement, Parameters.of("key", 900L)));
                    error(SqlServiceCode.PARAMETER_EXCEPTION, null,
                            () -> read(sql, statement, Parameters.of("key", "not-an-integer")));
                    expectRows(List.of(1000L), read(sql, statement, Parameters.of("key", 1000L)));
                }
                System.out.println("PROBE_OK bindings");
                return;
            }
            Path directory = Path.of(args[3]);
            List<Long> expected = Arrays.stream(args[4].split(",")).map(Long::valueOf).toList();
            try (var statement = sql.prepare(text).await()) {
                if (mode.equals("admitted-drop")) {
                    // The parent waits for held RPC DATA before issuing DROP, not for this marker alone.
                    signal(directory, "ready");
                    expectRows(expected, read(sql, statement));
                    error(SqlServiceCode.TARGET_NOT_FOUND_EXCEPTION, "has been dropped",
                            () -> read(sql, statement));
                } else {
                    expectRows(expected, read(sql, statement));
                    signal(directory, "ready");
                    await(directory, "changed");
                    if (mode.equals("worker-ddl")) {
                        error(SqlServiceCode.SQL_EXECUTION_EXCEPTION, "remote precondition not satisfied",
                                () -> read(sql, statement));
                    } else if (mode.equals("catalog-drop")) {
                        error(SqlServiceCode.TARGET_NOT_FOUND_EXCEPTION, "has been dropped",
                                () -> read(sql, statement));
                    } else throw new IllegalArgumentException(mode);
                    signal(directory, "rejected");
                    await(directory, "registered");
                    error(SqlServiceCode.TARGET_NOT_FOUND_EXCEPTION, "has been dropped",
                            () -> read(sql, statement));
                    literal(sql, text, expected);
                }
            }
            System.out.println("PROBE_OK " + mode);
        }
    }
}
