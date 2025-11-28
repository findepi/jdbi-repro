package com.example;

import org.jdbi.v3.core.Handle;
import org.jdbi.v3.core.HandleCallback;
import org.jdbi.v3.core.transaction.TransactionException;
import org.jdbi.v3.core.transaction.TransactionHandler;
import org.jdbi.v3.core.transaction.TransactionIsolationLevel;
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
            begin(handle);
            try {
                R result = callback.withHandle(handle);
                commit(handle);
                restoreAutoCommit(handle);
                return result;
            } catch (Exception e) {
                rollback(handle);

                System.out.println("Exception caught in transaction handler: " + e.getClass().getName() + ": " + e.getMessage());
                boolean isDeadlockException = isDeadlock(e);
                System.out.println("Is deadlock: " + isDeadlockException);

                if (attempt < MAX_RETRIES && isDeadlockException) {
                    System.out.println("Deadlock detected on attempt " + attempt + ", retrying...");
                    try {
                        Thread.sleep(50 * attempt); // Exponential backoff
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        restoreAutoCommit(handle);
                        throw new TransactionException("Interrupted during deadlock retry", ie);
                    }
                    continue;
                }

                System.out.println("Not retrying. Attempt: " + attempt + ", Max retries: " + MAX_RETRIES + ", Is deadlock: " + isDeadlockException);

                // Restore autocommit before rethrowing so the handle can be properly closed
                restoreAutoCommit(handle);

                if (e instanceof RuntimeException) {
                    throw (RuntimeException) e;
                } else {
                    throw (X) e;
                }
            }
        }
    }

    @Override
    public void begin(Handle handle)
    {
        try {
            handle.getConnection().setAutoCommit(false);
        } catch (SQLException e) {
            throw new TransactionException("Failed to begin transaction", e);
        }
    }

    @Override
    public void commit(Handle handle)
    {
        try {
            handle.getConnection().commit();
        } catch (SQLException e) {
            throw new TransactionException("Failed to commit transaction", e);
        }
    }

    @Override
    public void rollback(Handle handle)
    {
        try {
            handle.getConnection().rollback();
        } catch (SQLException e) {
            throw new TransactionException("Failed to rollback transaction", e);
        }
    }

    @Override
    public boolean isInTransaction(Handle handle)
    {
        try {
            return !handle.getConnection().getAutoCommit();
        } catch (SQLException e) {
            throw new TransactionException("Failed to check transaction status", e);
        }
    }

    @Override
    public void savepoint(Handle handle, String name)
    {
        handle.execute("SAVEPOINT " + name);
    }

    @Override
    public void rollbackToSavepoint(Handle handle, String name)
    {
        handle.execute("ROLLBACK TO SAVEPOINT " + name);
    }

    @Override
    public void releaseSavepoint(Handle handle, String name)
    {
        handle.execute("RELEASE SAVEPOINT " + name);
    }

    private void restoreAutoCommit(Handle handle)
    {
        try {
            handle.getConnection().setAutoCommit(true);
        } catch (SQLException e) {
            // Log but don't throw - we're likely in cleanup
            System.err.println("Warning: Failed to restore autocommit: " + e.getMessage());
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
