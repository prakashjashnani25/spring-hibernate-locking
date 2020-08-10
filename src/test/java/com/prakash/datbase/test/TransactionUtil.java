package com.prakash.datbase.test;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.Consumer;
import java.util.function.Function;

import org.hibernate.Session;
import org.hibernate.Transaction;
import org.springframework.dao.DataAccessException;

public abstract class TransactionUtil extends AbstractTest {

	
	@FunctionalInterface
    protected interface VoidCallable extends Callable<Void> {

        void execute();

        default Void call() throws Exception {
            execute();
            return null;
        }
    }
	
	@FunctionalInterface
	protected interface HibernateTransactionFunction<T> extends Function<Session, T> {
		default void beforeTransactionCompletion() {

		}

		default void afterTransactionCompletion() {

		}
	}

	@FunctionalInterface
	protected interface HibernateTransactionConsumer extends Consumer<Session> {
		default void beforeTransactionCompletion() {
			
		}

		default void afterTransactionCompletion() {

		}
	}

	protected <T> T doInTransaction(HibernateTransactionFunction<T> callable) {
		T result = null;
		Session session = null;
		Transaction txn = null;
		try {
			session = getSessionFactory().openSession();
			LOGGER.info("Before Transaction Function");
			callable.beforeTransactionCompletion();
			txn = session.beginTransaction();

			result = callable.apply(session);
			txn.commit();
			LOGGER.info("COMMIITING TRANSACTION FUNCTION");
		} catch (RuntimeException e) {
			if (txn != null && txn.isActive())
				txn.rollback();
			throw e;
		} finally {
			callable.afterTransactionCompletion();
			LOGGER.info("After Transaction Function ");
			if (session != null) {
				session.close();
			}
		}
		return result;
	}

	protected void doInTransaction(HibernateTransactionConsumer callable) {
		Session session = null;
		Transaction txn = null;
		try {
			session = getSessionFactory().openSession();
			LOGGER.info("Before Transaction Consumer");
			callable.beforeTransactionCompletion();
			txn = session.beginTransaction();

			callable.accept(session);
			LOGGER.info("COMMIITING TRANSACTION CONSUMER");
			txn.commit();
		} catch (RuntimeException e) {
			if (txn != null && txn.isActive())
				txn.rollback();
			throw e;
		} finally {
			callable.afterTransactionCompletion();
			LOGGER.info("After Transaction Consumer");
			if (session != null) {
				session.close();
			}
		}
	}
	
	protected void executeSync(VoidCallable callable) {
        executeSync(Collections.singleton(callable));
    }

    protected void executeSync(Collection<VoidCallable> callables) {
        try {
            List<Future<Void>> futures = executorService.invokeAll(callables);
            for (Future<Void> future : futures) {
                future.get();
            }
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    protected <T> void executeAsync1(Runnable callable, final Runnable completionCallback) {
        final Future future = executorService1.submit(callable);
        new Thread(() -> {
            while (!future.isDone()) {
                try {
                	LOGGER.info("SLEEPING");
                    Thread.sleep(100);
                } catch (Exception e) {
                    throw new IllegalStateException(e);
                }
            }
            try {
                completionCallback.run();
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        }).start();
    }

    protected Future<?> executeAsync1(Runnable callable) {
        return executorService1.submit(callable);
    }

    
    protected <T> void executeAsync(Runnable callable, final Runnable completionCallback) {
        final Future future = executorService.submit(callable);
        new Thread(() -> {
            while (!future.isDone()) {
                try {
                	LOGGER.info("SLEEPING");
                    Thread.sleep(100);
                } catch (Exception e) {
                    throw new IllegalStateException(e);
                }
            }
            try {
                completionCallback.run();
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        }).start();
    }

    protected Future<?> executeAsync(Runnable callable) {
        return executorService.submit(callable);
    }
    protected  void transact(Consumer<Connection> callback) {
        transact(callback, null);
    }

    protected  void transact(Consumer<Connection> callback, Consumer<Connection> before) {
        Connection connection = null;
        try {
            connection = newDataSource().getConnection();
            if (before != null) {
                before.accept(connection);
            }
            connection.setAutoCommit(false);
            callback.accept(connection);
            connection.commit();
        } catch (Exception e) {
            if (connection != null) {
                try {
                    connection.rollback();
                } catch (SQLException ex) {
                    throw new RuntimeException(e);
                }
            }
            throw (e instanceof DataAccessException ?
                    (DataAccessException) e : new RuntimeException(e));
        } finally {
            if(connection != null) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }
}
