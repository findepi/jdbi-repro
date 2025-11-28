package com.example;

import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.sqlobject.SqlObjectPlugin;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Testcontainers
public class PostgresContainerTest
{
    @Container
    private static final PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16-alpine")
            .withDatabaseName("testdb")
            .withUsername("testuser")
            .withPassword("testpass");

    private static String jdbcUrl;
    private static String username;
    private static String password;

    @BeforeAll
    public static void setupDatabase()
            throws Exception
    {
        jdbcUrl = postgres.getJdbcUrl();
        username = postgres.getUsername();
        password = postgres.getPassword();

        try (Connection connection = DriverManager.getConnection(jdbcUrl, username, password);
                Statement statement = connection.createStatement()) {

            statement.execute(
                    "CREATE TABLE users (" +
                            "id SERIAL PRIMARY KEY, " +
                            "name VARCHAR(100) NOT NULL, " +
                            "email VARCHAR(100) UNIQUE NOT NULL, " +
                            "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP" +
                            ")"
            );

            System.out.println("Database table created successfully");
        }
    }

    @Test
    public void testInsertAndQuery()
            throws Exception
    {
        try (Connection connection = DriverManager.getConnection(jdbcUrl, username, password);
                Statement statement = connection.createStatement()) {

            statement.execute("INSERT INTO users (name, email) VALUES ('John Doe', 'john@example.com')");

            var resultSet = statement.executeQuery("SELECT COUNT(*) FROM users");
            resultSet.next();
            int count = resultSet.getInt(1);

            assertTrue(count > 0, "Table should contain at least one record");
            System.out.println("Successfully inserted data. Row count: " + count);
        }
    }

    @Test
    public void testJdbiQuery()
    {
        Jdbi jdbi = Jdbi.create(jdbcUrl, username, password);

        jdbi.useHandle(handle -> {
            handle.execute("INSERT INTO users (name, email) VALUES (?, ?)", "Jane Smith", "jane@example.com");
        });

        Integer count = jdbi.withHandle(handle ->
                handle.createQuery("SELECT COUNT(*) FROM users WHERE name = :name")
                        .bind("name", "Jane Smith")
                        .mapTo(Integer.class)
                        .one()
        );

        assertEquals(1, count, "Should find exactly one user named Jane Smith");
        System.out.println("JDBI query successful. Found " + count + " user(s) named Jane Smith");
    }

    @Test
    public void testDeadlockRetry() throws Exception
    {
        // Configure JDBI with custom DeadlockRetryTransactionHandler to enable automatic retries on deadlock
        Jdbi jdbi = Jdbi.create(jdbcUrl, username, password);
        jdbi.setTransactionHandler(new DeadlockRetryTransactionHandler());

        // Create a test table with 100 rows
        jdbi.useHandle(handle -> {
            handle.execute("CREATE TABLE test_data (id INT PRIMARY KEY, value INT)");
            for (int i = 1; i <= 100; i++) {
                handle.execute("INSERT INTO test_data (id, value) VALUES (?, ?)", i, 0);
            }
        });

        System.out.println("Created test_data table with 100 rows");

        ExecutorService executor = Executors.newFixedThreadPool(2);
        CountDownLatch latch = new CountDownLatch(2);
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger attemptCount1 = new AtomicInteger(0);
        AtomicInteger attemptCount2 = new AtomicInteger(0);

        // Transaction 1: Updates row 1, then row 100
        Runnable transaction1 = () -> {
            try {
                jdbi.inTransaction(handle -> {
                    int attempt = attemptCount1.incrementAndGet();
                    System.out.println("Transaction 1 (attempt " + attempt + "): Updating row ID=1");
                    handle.execute("UPDATE test_data SET value = value + 1 WHERE id = 1");

                    latch.countDown();
                    System.out.println("Transaction 1 (attempt " + attempt + "): Waiting at latch");
                    latch.await();

                    Thread.sleep(100); // Give transaction 2 time to acquire its first lock

                    System.out.println("Transaction 1 (attempt " + attempt + "): Updating row ID=100");
                    handle.execute("UPDATE test_data SET value = value + 1 WHERE id = 100");

                    return null;
                });
                successCount.incrementAndGet();
                System.out.println("Transaction 1: Completed successfully after " + attemptCount1.get() + " attempt(s)");
            } catch (Exception e) {
                System.out.println("Transaction 1: Failed after " + attemptCount1.get() + " attempt(s) with " +
                                   e.getClass().getSimpleName() + ": " + e.getMessage());
                e.printStackTrace();
            }
        };

        // Transaction 2: Updates row 100, then row 1
        Runnable transaction2 = () -> {
            try {
                jdbi.inTransaction(handle -> {
                    int attempt = attemptCount2.incrementAndGet();
                    System.out.println("Transaction 2 (attempt " + attempt + "): Updating row ID=100");
                    handle.execute("UPDATE test_data SET value = value + 1 WHERE id = 100");

                    latch.countDown();
                    System.out.println("Transaction 2 (attempt " + attempt + "): Waiting at latch");
                    latch.await();

                    Thread.sleep(100); // Give transaction 1 time to acquire its first lock

                    System.out.println("Transaction 2 (attempt " + attempt + "): Updating row ID=1");
                    handle.execute("UPDATE test_data SET value = value + 1 WHERE id = 1");

                    return null;
                });
                successCount.incrementAndGet();
                System.out.println("Transaction 2: Completed successfully after " + attemptCount2.get() + " attempt(s)");
            } catch (Exception e) {
                System.out.println("Transaction 2: Failed after " + attemptCount2.get() + " attempt(s) with " +
                                   e.getClass().getSimpleName() + ": " + e.getMessage());
                e.printStackTrace();
            }
        };

        executor.submit(transaction1);
        executor.submit(transaction2);

        executor.shutdown();
        assertTrue(executor.awaitTermination(30, TimeUnit.SECONDS), "Transactions should complete within 30 seconds");

        System.out.println("Success count: " + successCount.get());
        System.out.println("Transaction 1 attempts: " + attemptCount1.get());
        System.out.println("Transaction 2 attempts: " + attemptCount2.get());
        int totalAttempts = attemptCount1.get() + attemptCount2.get();
        System.out.println("Total attempts: " + totalAttempts);

        // Verify that both transactions succeeded
        assertEquals(2, successCount.get(), "Both transactions should succeed with automatic retry");

        // Verify that retries occurred (total attempts should be more than 2 if there was a deadlock and retry)
        assertTrue(totalAttempts > 2, "Should have retried at least once due to deadlock (total attempts > 2)");

        // Clean up
        jdbi.useHandle(handle -> handle.execute("DROP TABLE test_data"));
    }

    @Test
    public void testDeadlockRetryWithDao() throws Exception
    {
        // Configure JDBI with custom DeadlockRetryTransactionHandler and SqlObject plugin
        Jdbi jdbi = Jdbi.create(jdbcUrl, username, password);
        jdbi.installPlugin(new SqlObjectPlugin());
        jdbi.setTransactionHandler(new DeadlockRetryTransactionHandler());

        // Create a test table with 100 rows
        jdbi.useHandle(handle -> {
            handle.execute("CREATE TABLE test_data_dao (id INT PRIMARY KEY, value INT)");
            for (int i = 1; i <= 100; i++) {
                handle.execute("INSERT INTO test_data_dao (id, value) VALUES (?, ?)", i, 0);
            }
        });

        System.out.println("Created test_data_dao table with 100 rows for DAO test");

        ExecutorService executor = Executors.newFixedThreadPool(2);
        CountDownLatch latch = new CountDownLatch(2);
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger attemptCount1 = new AtomicInteger(0);
        AtomicInteger attemptCount2 = new AtomicInteger(0);

        // Transaction 1: Updates row 1, then row 100 using DAO
        Runnable transaction1 = () -> {
            try {
                TestDataDao dao = jdbi.onDemand(TestDataDao.class);

                System.out.println("Transaction 1: Starting DAO transaction");

                dao.updateRowsInTransaction(1, 100, latch, attemptCount1);

                successCount.incrementAndGet();
                System.out.println("Transaction 1: Completed successfully after " + attemptCount1.get() + " attempt(s)");
            } catch (Exception e) {
                System.out.println("Transaction 1: Failed after " + attemptCount1.get() + " attempt(s) with " +
                                   e.getClass().getSimpleName() + ": " + e.getMessage());
                e.printStackTrace();
            }
        };

        // Transaction 2: Updates row 100, then row 1 using DAO
        Runnable transaction2 = () -> {
            try {
                TestDataDao dao = jdbi.onDemand(TestDataDao.class);

                System.out.println("Transaction 2: Starting DAO transaction");

                dao.updateRowsInTransaction(100, 1, latch, attemptCount2);

                successCount.incrementAndGet();
                System.out.println("Transaction 2: Completed successfully after " + attemptCount2.get() + " attempt(s)");
            } catch (Exception e) {
                System.out.println("Transaction 2: Failed after " + attemptCount2.get() + " attempt(s) with " +
                                   e.getClass().getSimpleName() + ": " + e.getMessage());
                e.printStackTrace();
            }
        };

        executor.submit(transaction1);
        executor.submit(transaction2);

        executor.shutdown();
        assertTrue(executor.awaitTermination(30, TimeUnit.SECONDS), "Transactions should complete within 30 seconds");

        System.out.println("Success count: " + successCount.get());
        System.out.println("Transaction 1 attempts: " + attemptCount1.get());
        System.out.println("Transaction 2 attempts: " + attemptCount2.get());
        int totalAttempts = attemptCount1.get() + attemptCount2.get();
        System.out.println("Total attempts: " + totalAttempts);

        // Verify that both transactions succeeded
        assertEquals(2, successCount.get(), "Both transactions should succeed with automatic retry");

        // Verify that retries occurred (total attempts should be more than 2 if there was a deadlock and retry)
        assertTrue(totalAttempts > 2, "Should have retried at least once due to deadlock (total attempts > 2)");

        // Clean up
        jdbi.useHandle(handle -> handle.execute("DROP TABLE test_data_dao"));
    }
}
