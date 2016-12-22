package net.coderbee.mybatis.batch;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.Reader;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

import javax.sql.DataSource;

import org.apache.ibatis.datasource.unpooled.UnpooledDataSource;
import org.apache.ibatis.io.Resources;
import org.apache.ibatis.jdbc.ScriptRunner;

/**
 * @author coderbee 2016年12月21日 下午7:36:21
 *
 */
public class TestBatchBase {

	protected static final String derby_schema = "sql/derby/derby-schema.sql";
	protected static final String derby_dataload = "sql/derby/derby-data.sql";

	protected static final String hsqldb_schema = "sql/hsqldb/hsqldb-schema.sql";
	protected static final String hsqldb_dataload = "sql/hsqldb/hsqldb-data.sql";

	public static UnpooledDataSource createUnpooledDataSource(String resource,
			String dbName)
					throws IOException {
		Properties props = Resources.getResourceAsProperties(resource);
		UnpooledDataSource ds = new UnpooledDataSource();
		ds.setDriver(props.getProperty(dbName + ".driver"));
		ds.setUrl(props.getProperty(dbName + ".url"));
		ds.setUsername(props.getProperty(dbName + ".username"));
		ds.setPassword(props.getProperty(dbName + ".password"));
		return ds;
	}

	public static void runScript(DataSource ds, String resource)
			throws IOException, SQLException {
		Connection connection = ds.getConnection();
		try {
			ScriptRunner runner = new ScriptRunner(connection);
			runner.setAutoCommit(true);
			runner.setStopOnError(false);
			runner.setLogWriter(new PrintWriter(System.out));
			runner.setErrorLogWriter(new PrintWriter(System.err));
			runScript(runner, resource);
		} finally {
			connection.close();
		}
	}

	public static void runScript(ScriptRunner runner, String resource)
			throws IOException, SQLException {
		Reader reader = Resources.getResourceAsReader(resource);
		try {
			runner.runScript(reader);
		} finally {
			reader.close();
		}
	}

	public static DataSource createDerbyDataSource()
			throws IOException, SQLException {
		DataSource ds = createUnpooledDataSource("db.properties", "derby");
		runScript(ds, derby_schema);
		runScript(ds, derby_dataload);
		return ds;
	}

	public static DataSource createHsqldbDataSource()
			throws IOException, SQLException {
		DataSource ds = createUnpooledDataSource("db.properties", "hsqldb");
		runScript(ds, hsqldb_schema);
		runScript(ds, hsqldb_dataload);
		return ds;
	}

}
