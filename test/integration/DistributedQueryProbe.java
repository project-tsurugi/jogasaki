/* Copyright 2026 Project Tsurugi. Licensed under the Apache License, Version 2.0. */
import com.tsurugidb.sql.proto.SqlRequest.TransactionOption;
import com.tsurugidb.sql.proto.SqlRequest.TransactionType;
import com.tsurugidb.tsubakuro.common.SessionBuilder;
import com.tsurugidb.tsubakuro.sql.SqlClient;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;

/** Small client for explain, cancellation before response, and a slow result consumer. */
class DistributedQueryProbe {
    private static void trigger(String file) throws Exception {
        var until = Instant.now().plus(Duration.ofSeconds(45));
        while (!Files.exists(Path.of(file))) {
            if (Instant.now().isAfter(until)) throw new IllegalStateException("No test trigger");
            Thread.sleep(20);
        }
    }
    public static void main(String[] args) throws Exception {
        try (var session = SessionBuilder.connect(args[1]).create(); var sql = SqlClient.attach(session)) {
            if (args[0].equals("explain")) {
                System.out.println(sql.explain(args[2]).await().getContents());
                return;
            }
            var option = TransactionOption.newBuilder().setType(TransactionType.READ_ONLY).build();
            try (var tx = sql.createTransaction(option).await()) {
                try (var pending = tx.executeQuery(args[2])) {
                    System.out.println("SQL_SUBMITTED");
                    if (args[0].equals("slow")) {
                        try (var result = pending.await()) {
                            if (!result.nextRow()) throw new IllegalStateException("Expected a row");
                            System.out.println("ROW_READY");
                            trigger(args[3]);
                        }
                    } else {
                        trigger(args[3]);
                    }
                    System.out.println("CANCEL_BEGIN");
                }
                System.out.println("CANCEL_SENT");
                try { tx.rollback().await(); }
                catch (Exception e) { System.out.println("ROLLBACK_RESULT " + e.getClass().getSimpleName()); }
            }
        }
    }
}
