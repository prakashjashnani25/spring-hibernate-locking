package com.prakash.datbase.test;

import java.math.BigDecimal;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;

import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;
import javax.persistence.Version;

import org.hibernate.LockMode;
import org.hibernate.LockOptions;
import org.hibernate.Session;
import org.hibernate.StaleObjectStateException;
import org.junit.Before;
import org.junit.Test;

public class TestSessionLock extends TransactionUtil {

	public static final int WAIT_MILLIS = 500;

	private static interface ProductLockRequestCallable {
		void lock(Session session, Product product);
	}

	private final CountDownLatch endLatch = new CountDownLatch(1);

	@Override
	protected Class<?>[] entities() {
		return new Class<?>[] { Product.class };
	}

	@Override
	protected DataSourceProvider getDataSourceProvider() {
		return new HsqldbDataSourceProvider();
	}

	@Before
	public void init() {
		super.init();
		doInTransaction(session -> {
			Product product = new Product();
			product.setId(1L);
			product.setDescription("USB Flash Drive");
			product.setPrice(BigDecimal.valueOf(12.99));
			session.persist(product);
		});
	}

	private void testPessimisticLocking(ProductLockRequestCallable primaryLockRequestCallable,
			ProductLockRequestCallable secondaryLockRequestCallable) {
		doInTransaction(session -> {
			try {
				Product product = (Product) session.get(Product.class, 1L);
				primaryLockRequestCallable.lock(session, product);
				executeAsync(() -> {
					doInTransaction(_session -> {
						Product _product = (Product) _session.get(Product.class, 1L);
						secondaryLockRequestCallable.lock(_session, _product);
					});
				}, endLatch::countDown);
				sleep(WAIT_MILLIS);
			} catch (StaleObjectStateException e) {
				LOGGER.info("Optimistic locking failure: ", e);
			}
		});
		awaitOnLatch(endLatch);
	}

	private void testPessimisticLockingAsynch(ProductLockRequestCallable primaryLockRequestCallable,
			ProductLockRequestCallable secondaryLockRequestCallable) {

		executeAsync(() -> {
			doInTransaction(_session -> {
				Product _product = (Product) _session.get(Product.class, 1L);
				primaryLockRequestCallable.lock(_session, _product);
			});
		});
		executeAsync1(() -> {
			doInTransaction(_session -> {
				Product _product = (Product) _session.get(Product.class, 1L);
				secondaryLockRequestCallable.lock(_session, _product);
			});
		}, endLatch::countDown);
		sleep(WAIT_MILLIS);

		awaitOnLatch(endLatch);
	}

	@Test
	public void testPessimisticReadDoesNotBlockPessimisticRead() throws InterruptedException {
		LOGGER.info("Test PESSIMISTIC_READ doesn't block PESSIMISTIC_READ");
		testPessimisticLocking((session, product) -> {
			session.buildLockRequest(new LockOptions(LockMode.PESSIMISTIC_READ)).lock(product);
			LOGGER.info("PESSIMISTIC_READ acquired");
		}, (session, product) -> {
			session.buildLockRequest(new LockOptions(LockMode.PESSIMISTIC_READ)).lock(product);
			LOGGER.info("PESSIMISTIC_READ acquired");
		});
	}

	@Test
	public void testPessimisticReadBlocksUpdate() throws InterruptedException {
		LOGGER.info("Test PESSIMISTIC_READ blocks UPDATE");
		testPessimisticLocking((session, product) -> {
			session.buildLockRequest(new LockOptions(LockMode.PESSIMISTIC_READ)).lock(product);
			LOGGER.info("PESSIMISTIC_READ acquired");
		}, (session, product) -> {
			product.setDescription("USB Flash Memory Stick");
			session.flush();
			LOGGER.info("Implicit lock acquired");
		});
	}

	@Test
	public void testPessimisticReadWithPessimisticWriteNoWait() throws InterruptedException {
		LOGGER.info("Test PESSIMISTIC_READ blocks PESSIMISTIC_WRITE, NO WAIT fails fast");
		testPessimisticLocking((session, product) -> {
			session.buildLockRequest(new LockOptions(LockMode.PESSIMISTIC_READ)).lock(product);
			LOGGER.info("PESSIMISTIC_READ acquired");
		}, (session, product) -> {
			session.buildLockRequest(new LockOptions(LockMode.PESSIMISTIC_WRITE))
					.setTimeOut(Session.LockRequest.PESSIMISTIC_NO_WAIT).lock(product);
			LOGGER.info("PESSIMISTIC_WRITE acquired");
		});
	}

	@Test
	public void testPessimisticWriteBlocksPessimisticRead() throws InterruptedException {
		LOGGER.info("Test PESSIMISTIC_WRITE blocks PESSIMISTIC_READ");
		testPessimisticLockingAsynch((session, product) -> {
			session.buildLockRequest(new LockOptions(LockMode.PESSIMISTIC_WRITE)).lock(product);
			LOGGER.info("PESSIMISTIC_WRITE acquired");
		}, (session, product) -> {
			session.buildLockRequest(new LockOptions(LockMode.PESSIMISTIC_READ)).lock(product);
			LOGGER.info("PESSIMISTIC_READ acquired");
		});
	}

	@Test
	public void testPessimisticWriteBlocksPessimisticWrite() throws InterruptedException {
		LOGGER.info("Test PESSIMISTIC_WRITE blocks PESSIMISTIC_WRITE");
		testPessimisticLocking((session, product) -> {
			session.buildLockRequest(new LockOptions(LockMode.PESSIMISTIC_WRITE)).lock(product);
			LOGGER.info("PESSIMISTIC_WRITE acquired");
		}, (session, product) -> {
			session.buildLockRequest(new LockOptions(LockMode.PESSIMISTIC_WRITE)).lock(product);
			LOGGER.info("PESSIMISTIC_WRITE acquired");
		});
	}

	protected void awaitOnLatch(CountDownLatch latch) {
		try {
			LOGGER.info("COUNT Down Latch Awaiting"+latch.getCount());
			latch.await();
			LOGGER.info("COUNT Down Latch wait complete"+latch.getCount());
		} catch (InterruptedException e) {
			throw new IllegalStateException(e);
		}
	}

	protected void sleep(int millis) {
		sleep(millis, null);
	}

	protected <V> V sleep(int millis, Callable<V> callable) {
		V result = null;
		try {
			// LOGGER.debug("Wait {} ms!", millis);
			if (callable != null) {
				result = callable.call();
			}
			Thread.sleep(millis);
		} catch (Exception e) {
			throw new IllegalStateException(e);
		}
		return result;
	}

	@Entity(name = "Product")
	@Table(name = "product")
	public static class Product {

		@Id
		private Long id;

		private String description;

		private BigDecimal price;

		@Version
		private int version;

		public Long getId() {
			return id;
		}

		public void setId(Long id) {
			this.id = id;
		}

		public String getDescription() {
			return description;
		}

		public void setDescription(String description) {
			this.description = description;
		}

		public BigDecimal getPrice() {
			return price;
		}

		public void setPrice(BigDecimal price) {
			this.price = price;
		}
	}
}
