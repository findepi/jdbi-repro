package com.example;

import org.jdbi.v3.sqlobject.customizer.Bind;
import org.jdbi.v3.sqlobject.statement.SqlQuery;
import org.jdbi.v3.sqlobject.statement.SqlUpdate;
import org.jdbi.v3.sqlobject.transaction.Transaction;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

public interface TestDataDao
{
    @SqlUpdate("UPDATE test_data_dao SET value = value + 1 WHERE id = :id")
    void updateRow(@Bind("id") int id);

    @Transaction
    default void updateRowsInTransaction(int id1, int id2, CountDownLatch latch, AtomicInteger attemptCount) throws Exception
    {
        int attempt = attemptCount.incrementAndGet();
        System.out.println("DAO (attempt " + attempt + "): Updating row ID=" + id1);
        updateRow(id1);

        latch.countDown();
        System.out.println("DAO (attempt " + attempt + "): Waiting at latch");
        latch.await();

        Thread.sleep(100);

        System.out.println("DAO (attempt " + attempt + "): Updating row ID=" + id2);
        updateRow(id2);
    }

    @SqlQuery("SELECT value FROM test_data_dao WHERE id = :id")
    int getValue(@Bind("id") int id);
}
