package com.example;

import org.jdbi.v3.core.Handle;
import org.jdbi.v3.core.transaction.TransactionException;
import org.jdbi.v3.core.transaction.TransactionHandler;
import org.postgresql.util.PSQLException;

import java.sql.SQLException;

public class DeadlockRetryTransactionHandler implements TransactionHandler
{
    private static final int MAX_RETRIES = 5;
    private static final String DEADLOCK_SQLSTATE = "40P01";

    @Override
    public <R, X extends Exception> R inTransaction(Handle handle, HandleCallback<R, X> callback) throws X
    {
        return inTransaction(handle, TransactionIsolationLevel.READ_COMMITTED, callback);
    }

    @Override
    public <R, X extends Exception> R inTransaction(Handle handle, TransactionIsolationLevel level, HandleCallback<R, X> callback) throws X
    {
        int attempt = 0;
        while (true) {
            attempt++;
            handle.begin();
            try {
                R result = callback.withHandle(handle);
                handle.commit();
                return result;
            } catch (Exception e) {
                handle.rollback();

                System.out.println("Exception caught in transaction handler: " + e.getClass().getName() + ": " + e.getMessage());
                boolean isDeadlockException = isDeadlock(e);
                System.out.println("Is deadlock: " + isDeadlockException);

                if (attempt < MAX_RETRIES && isDeadlockException) {
                    System.out.println("Deadlock detected on attempt " + attempt + ", retrying...");
                    try {
                        Thread.sleep(50 * attempt); // Exponential backoff
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw new TransactionException("Interrupted during deadlock retry", ie);
                    }
                    continue;
                }

                System.out.println("Not retrying. Attempt: " + attempt + ", Max retries: " + MAX_RETRIES + ", Is deadlock: " + isDeadlockException);

                if (e instanceof RuntimeException) {
                    throw (RuntimeException) e;
                } else {
                    throw (X) e;
                }
            }
        }
    }

    private boolean isDeadlock(Exception e)
    {
        Throwable current = e;
        while (current != null) {
            if (current instanceof PSQLException) {
                PSQLException psqlException = (PSQLException) current;
                System.out.println("Found PSQLException with SQLState: " + psqlException.getSQLState());
                if (DEADLOCK_SQLSTATE.equals(psqlException.getSQLState())) {
                    return true;
                }
            }
            if (current instanceof SQLException) {
                SQLException sqlException = (SQLException) current;
                System.out.println("Found SQLException with SQLState: " + sqlException.getSQLState());
                if (DEADLOCK_SQLSTATE.equals(sqlException.getSQLState())) {
                    return true;
                }
            }
            current = current.getCause();
        }
        return false;
    }
}
