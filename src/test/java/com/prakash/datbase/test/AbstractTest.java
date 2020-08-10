package com.prakash.datbase.test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import javax.persistence.EntityManagerFactory;
import javax.sql.DataSource;

import org.hibernate.Interceptor;
import org.hibernate.SessionFactory;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.cfg.Configuration;
import org.hsqldb.jdbc.JDBCDataSource;
import org.junit.After;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.ttddyy.dsproxy.ExecutionInfo;
import net.ttddyy.dsproxy.QueryInfo;
import net.ttddyy.dsproxy.listener.ChainListener;
import net.ttddyy.dsproxy.listener.DefaultQueryLogEntryCreator;
import net.ttddyy.dsproxy.listener.SLF4JQueryLoggingListener;
import net.ttddyy.dsproxy.support.ProxyDataSourceBuilder;

public abstract class AbstractTest{
	
	protected final Logger LOGGER = LoggerFactory.getLogger(getClass());
	private EntityManagerFactory emf;
	private SessionFactory sf;
	protected final ExecutorService executorService = Executors.newSingleThreadExecutor(r -> {
        Thread bob = new Thread(r);
        bob.setName("User 1");
        return bob;
    });
	protected final ExecutorService executorService1 = Executors.newSingleThreadExecutor(r -> {
        Thread bob = new Thread(r);
        bob.setName("User 2");
        return bob;
    });
	
	@Before
	public void init() {
		if (nativeHibernateSessionFactoryBootstrap()) {
			sf = newSessionFactory();
		} else {
//			emf = newEntityManagerFactory();
		}
	}

	private SessionFactory newSessionFactory() {
		Properties properties = getProperties();
		Configuration configuration = new Configuration().addProperties(properties);
        for(Class<?> entityClass : entities()) {
            configuration.addAnnotatedClass(entityClass);
        }
        String[] packages = packages();
        if(packages != null) {
            for(String scannedPackage : packages) {
                configuration.addPackage(scannedPackage);
            }
        }
        Interceptor interceptor = interceptor();
        if(interceptor != null) {
            configuration.setInterceptor(interceptor);
        }
        return configuration.buildSessionFactory(
                new StandardServiceRegistryBuilder()
                        .applySettings(properties)
                        .build()
        );
	}

	protected abstract Class<?>[] entities();

    protected List<String> entityClassNames() {
        return Arrays.asList(entities()).stream().map(Class::getName).collect(Collectors.toList());
    }

    protected String[] packages() {
        return null;
    }

    protected Interceptor interceptor() {
        return null;
    }
	@After
	public void destroy() {
		if (nativeHibernateSessionFactoryBootstrap()) {
			sf.close();
		} else {
			emf.close();
		}
	}

	public EntityManagerFactory getEntityManagerFactory() {
		return emf;
	}

	public SessionFactory getSessionFactory() {
		return nativeHibernateSessionFactoryBootstrap() ? sf : emf.unwrap(SessionFactory.class);
	}

	protected boolean nativeHibernateSessionFactoryBootstrap() {
		return true;
	}

	protected DataSourceProvider getDataSourceProvider() {
		return new HsqldbDataSourceProvider();
	}

	protected Properties getProperties() {
		Properties properties = new Properties();
		properties.put("hibernate.dialect", getDataSourceProvider().hibernateDialect());
		properties.put("hibernate.hbm2ddl.auto", "create-drop");
		// log settings
		// properties.put("hibernate.show_sql", Boolean.TRUE.toString());
		// properties.put("hibernate.format_sql", Boolean.TRUE.toString());
		// properties.put("hibernate.use_sql_coments", Boolean.FALSE.toString());
		properties.put("hibernate.generate_statistics", Boolean.TRUE.toString());

		// data source settings
		properties.put("hibernate.connection.datasource", newDataSource());
		return properties;
	}

	protected DataSource newDataSource() {
		if (proxyDataSource()) {
			ChainListener listener = new ChainListener();
			SLF4JQueryLoggingListener loggingListener = new SLF4JQueryLoggingListener();
			loggingListener.setQueryLogEntryCreator(new AbstractTest.InlineQueryLogEntryCreator());
			listener.addListener(loggingListener);
			return ProxyDataSourceBuilder.create(getDataSourceProvider().dataSource()).name(getClass().getName())
					.listener(listener).build();
		} else {
			return getDataSourceProvider().dataSource();
		}
	}

	public static class InlineQueryLogEntryCreator extends DefaultQueryLogEntryCreator {
		@Override
		protected void writeParamsEntry(StringBuilder sb, ExecutionInfo execInfo, List<QueryInfo> queryInfoList) {
			sb.append("Params:[");
			for (QueryInfo queryInfo : queryInfoList) {
				boolean firstArg = true;
				for (Map<String, Object> paramMap : queryInfo.getQueryArgsList()) {

					if (!firstArg) {
						sb.append(", ");
					} else {
						firstArg = false;
					}

					SortedMap<String, Object> sortedParamMap = new TreeMap<>(new StringAsIntegerComparator());
					sortedParamMap.putAll(paramMap);

					sb.append("(");
					boolean firstParam = true;
					for (Map.Entry<String, Object> paramEntry : sortedParamMap.entrySet()) {
						if (!firstParam) {
							sb.append(", ");
						} else {
							firstParam = false;
						}
						Object parameter = paramEntry.getValue();
						if (parameter != null && parameter.getClass().isArray()) {
							sb.append(arrayToString(parameter));
						} else {
							sb.append(parameter);
						}
					}
					sb.append(")");
				}
			}
			sb.append("]");
		}

		private String arrayToString(Object object) {
			if (object.getClass().isArray()) {
				if (object instanceof byte[]) {
					return Arrays.toString((byte[]) object);
				}
				if (object instanceof short[]) {
					return Arrays.toString((short[]) object);
				}
				if (object instanceof char[]) {
					return Arrays.toString((char[]) object);
				}
				if (object instanceof int[]) {
					return Arrays.toString((int[]) object);
				}
				if (object instanceof long[]) {
					return Arrays.toString((long[]) object);
				}
				if (object instanceof float[]) {
					return Arrays.toString((float[]) object);
				}
				if (object instanceof double[]) {
					return Arrays.toString((double[]) object);
				}
				if (object instanceof boolean[]) {
					return Arrays.toString((boolean[]) object);
				}
				if (object instanceof Object[]) {
					return Arrays.toString((Object[]) object);
				}
			}
			throw new UnsupportedOperationException("Arrat type not supported: " + object.getClass());
		}
	};

	protected boolean proxyDataSource() {
		return true;
	}

	protected interface DataSourceProvider {

		enum IdentifierStrategy {
			IDENTITY, SEQUENCE
		}

		enum Database {
			HSQLDB, POSTGRESQL, ORACLE, MYSQL, SQLSERVER
		}

		String hibernateDialect();

		DataSource dataSource();

		Class<? extends DataSource> dataSourceClassName();

		Properties dataSourceProperties();

		List<IdentifierStrategy> identifierStrategies();

		Database database();
	}

	public static class HsqldbDataSourceProvider implements DataSourceProvider {

		@Override
		public String hibernateDialect() {
			return "org.hibernate.dialect.HSQLDialect";
		}

		@Override
		public DataSource dataSource() {
			JDBCDataSource dataSource = new JDBCDataSource();
			dataSource.setUrl("jdbc:hsqldb:mem:test");
			dataSource.setUser("sa");
			dataSource.setPassword("");
			return dataSource;
		}

		@Override
		public Class<? extends DataSource> dataSourceClassName() {
			return JDBCDataSource.class;
		}

		@Override
		public Properties dataSourceProperties() {
			Properties properties = new Properties();
			properties.setProperty("url", "jdbc:hsqldb:mem:test");
			properties.setProperty("user", "sa");
			properties.setProperty("password", "");
			return properties;
		}

		@Override
		public List<IdentifierStrategy> identifierStrategies() {
			return Arrays.asList(IdentifierStrategy.IDENTITY, IdentifierStrategy.SEQUENCE);
		}

		@Override
		public Database database() {
			return Database.HSQLDB;
		}
	}

}
