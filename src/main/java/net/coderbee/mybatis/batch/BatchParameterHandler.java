package net.coderbee.mybatis.batch;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;

import org.apache.ibatis.executor.keygen.NoKeyGenerator;
import org.apache.ibatis.executor.parameter.DefaultParameterHandler;
import org.apache.ibatis.executor.parameter.ParameterHandler;
import org.apache.ibatis.mapping.BoundSql;
import org.apache.ibatis.mapping.MappedStatement;
import org.apache.ibatis.plugin.Interceptor;
import org.apache.ibatis.plugin.Intercepts;
import org.apache.ibatis.plugin.Invocation;
import org.apache.ibatis.plugin.Plugin;
import org.apache.ibatis.plugin.Signature;

/**
 * 用于绑定参数到 {@link java.sql.PreparedStatement} 的拦截器。<br/>
 * <br/>
 * 
 * 拦截参数是 {@link net.coderbee.mybatis.batch.BatchParameter}
 * 的实例的参数绑定，取出里面的实体列表，分批绑定并执行。
 * 
 * @author <a href="http://coderbee.net">coderbee</a>
 *
 */
@Intercepts({
		@Signature(type = ParameterHandler.class, method = "setParameters",
				args = {
						PreparedStatement.class }) })
public class BatchParameterHandler implements Interceptor {

	public Object intercept(Invocation invocation) throws Throwable {
		if (invocation.getTarget() instanceof DefaultParameterHandler) {
			DefaultParameterHandler parameterHandler = (DefaultParameterHandler) invocation
					.getTarget();

			MappedStatement mappedStatement = (MappedStatement) ReflectHelper
					.getValueByFieldName(parameterHandler, "mappedStatement");

			Object paramObj = ReflectHelper
					.getValueByFieldName(parameterHandler, "parameterObject");
			if (paramObj instanceof BatchParameter) {

				PreparedStatement ps = (PreparedStatement) invocation
						.getArgs()[0];
				BoundSql boundSql = (BoundSql) ReflectHelper
						.getValueByFieldName(parameterHandler, "boundSql");

				@SuppressWarnings({ "unchecked", "rawtypes" })
				List<Object> parameterObject = (List) ((BatchParameter) paramObj)
						.getData();

				ps.clearBatch();
				ps.clearParameters();

				prepareBatchExecuteContext();

				int i = executeBatch(mappedStatement, paramObj, ps, boundSql,
						parameterObject);

				return i;
			}
		}

		return invocation.proceed();
	}

	private void prepareBatchExecuteContext() {
		BatchExecuteContext.getGeneratedKeys().clear();
		BatchExecuteContext.getUpdatedResult().clear();
	}

	protected int executeBatch(MappedStatement mappedStatement, Object paramObj,
			PreparedStatement ps, BoundSql boundSql,
			List<Object> parameterObject) throws SQLException {
		int parameterCount = parameterObject.size();

		boolean generatedKeys = !(mappedStatement
				.getKeyGenerator() instanceof NoKeyGenerator);

		@SuppressWarnings("rawtypes")
		int batchSize = ((BatchParameter) paramObj).getBatchSize();
		int i = 0;
		for (Object pobject : parameterObject) {
			DefaultParameterHandler handler = new DefaultParameterHandler(
					mappedStatement, pobject, boundSql);
			handler.setParameters(ps);
			ps.addBatch();
			i += 1;
			if (i % batchSize == 0) {
				executeBatch(ps, generatedKeys, parameterCount);

			}
		}
		if (parameterObject.size() % batchSize != 0) {
			executeBatch(ps, generatedKeys, parameterCount);
		}
		return i;
	}

	protected void executeBatch(PreparedStatement ps, boolean generatedKeys,
			int parameterCount)
					throws SQLException {
		int[] batch = ps.executeBatch();
		BatchExecuteContext.addUpdatedRowCount(batch,
				parameterCount);

		if (generatedKeys) {
			ResultSet keySet = ps.getGeneratedKeys();
			BatchExecuteContext.addGeneratedKeys(keySet,
					parameterCount);
		}
	}

	public Object plugin(Object target) {
		return Plugin.wrap(target, this);
	}

	public void setProperties(Properties properties) {
	}

}
