package net.coderbee.mybatis.batch;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.ibatis.executor.Executor;
import org.apache.ibatis.executor.keygen.Jdbc3KeyGenerator;
import org.apache.ibatis.executor.keygen.KeyGenerator;
import org.apache.ibatis.executor.keygen.SelectKeyGenerator;
import org.apache.ibatis.executor.parameter.DefaultParameterHandler;
import org.apache.ibatis.executor.statement.PreparedStatementHandler;
import org.apache.ibatis.executor.statement.RoutingStatementHandler;
import org.apache.ibatis.executor.statement.StatementHandler;
import org.apache.ibatis.mapping.BoundSql;
import org.apache.ibatis.mapping.MappedStatement;
import org.apache.ibatis.plugin.Interceptor;
import org.apache.ibatis.plugin.Intercepts;
import org.apache.ibatis.plugin.Invocation;
import org.apache.ibatis.plugin.Plugin;
import org.apache.ibatis.plugin.Signature;
import org.apache.ibatis.reflection.MetaObject;
import org.apache.ibatis.session.Configuration;

/**
 * 拦截参数是 {@link net.coderbee.mybatis.batch.BatchParameter}
 * 的实例的语句执行，由于批量插入在参数绑定的时候已经执行了，这里不需要再次执行。
 * 
 * @author <a href="http://coderbee.net">coderbee</a>
 *
 */
@Intercepts({
		@Signature(type = StatementHandler.class, method = "update", args = {
				Statement.class }) })
public class BatchStatementHandler implements Interceptor {

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public Object intercept(Invocation invocation) throws Throwable {
		if (invocation.getTarget() instanceof RoutingStatementHandler) {
			RoutingStatementHandler routingStatementHandler = (RoutingStatementHandler) invocation
					.getTarget();

			PreparedStatement ps = (PreparedStatement) invocation.getArgs()[0];
			StatementHandler delegate = (StatementHandler) ReflectHelper
					.getValueByFieldName(routingStatementHandler, "delegate");
			if (delegate instanceof PreparedStatementHandler) {

				PreparedStatementHandler preparedStatementHandler = (PreparedStatementHandler) delegate;

				BoundSql boundSql = (BoundSql) ReflectHelper
						.getValueByFieldName(preparedStatementHandler,
								"boundSql");

				Object parameterObject = boundSql.getParameterObject();

				if (parameterObject instanceof BatchParameter) {
					MappedStatement mappedStatement = (MappedStatement) ReflectHelper
							.getValueByFieldName(preparedStatementHandler,
									"mappedStatement");

					KeyGeneratorType keyGeneratorType = getKeyGeneratorType(
							mappedStatement);

					// 如果设置了，提前获取 key
					if (keyGeneratorType == KeyGeneratorType.SELECT_BEFORE) {
						preGenerateKey(ps, preparedStatementHandler,
								parameterObject, mappedStatement,
								keyGeneratorType);
					}

					// 清理批量执行的上下文
					prepareBatchExecuteContext();

					// 批量执行
					executeBatch(mappedStatement,
							(BatchParameter) parameterObject, ps,
							boundSql, keyGeneratorType);

					// 获取受影响的总共行数
					int count = getAffectedRowCount();
					return count;
				}
			}
		}

		return invocation.proceed();
	}

	protected KeyGeneratorType getKeyGeneratorType(
			MappedStatement mappedStatement)
					throws NoSuchFieldException, IllegalAccessException {
		KeyGenerator keyGenerator = mappedStatement
				.getKeyGenerator();
		KeyGeneratorType keyGeneratorType = KeyGeneratorType.NONE;
		if (keyGenerator instanceof SelectKeyGenerator) {
			Boolean executeBefore = (Boolean) ReflectHelper
					.getValueByFieldName(keyGenerator,
							"executeBefore");
			if (executeBefore) {
				keyGeneratorType = KeyGeneratorType.SELECT_BEFORE;
			} else {
				keyGeneratorType = KeyGeneratorType.SELECT_AFTER;
			}
		} else if (keyGenerator instanceof Jdbc3KeyGenerator) {
			keyGeneratorType = KeyGeneratorType.SELECT_AFTER;
		}
		return keyGeneratorType;
	}

	@SuppressWarnings("rawtypes")
	protected void preGenerateKey(PreparedStatement ps,
			PreparedStatementHandler preparedStatementHandler,
			Object parameterObject, MappedStatement mappedStatement,
			KeyGeneratorType keyGeneratorType)
					throws NoSuchFieldException, IllegalAccessException {
		Executor executor = (Executor) ReflectHelper
				.getValueByFieldName(preparedStatementHandler,
						"executor");

		KeyGenerator keyGenerator = mappedStatement
				.getKeyGenerator();
		for (Object oneParam : ((BatchParameter) parameterObject)
				.getData()) {
			((SelectKeyGenerator) keyGenerator)
					.processBefore(executor,
							mappedStatement, ps,
							oneParam);
		}
	}

	protected int executeBatch(MappedStatement mappedStatement,
			BatchParameter<Object> paramObj,
			PreparedStatement ps, BoundSql boundSql,
			KeyGeneratorType keyGeneratorType) throws SQLException,
					SecurityException, NoSuchFieldException,
					IllegalArgumentException, IllegalAccessException {

		int batchSize = paramObj.getBatchSize();
		List<Object> parameterObject = paramObj.getData();
		List<Object> batchParams = new ArrayList<Object>(batchSize);

		for (Object pobject : parameterObject) {
			DefaultParameterHandler handler = new DefaultParameterHandler(
					mappedStatement, pobject, boundSql);
			handler.setParameters(ps);
			ps.addBatch();

			batchParams.add(pobject);
			if (batchParams.size() == batchSize) {
				executeBatch(mappedStatement, ps, keyGeneratorType,
						batchParams);
				batchParams.clear();
			}
		}
		if (parameterObject.size() % batchSize != 0) {
			executeBatch(mappedStatement, ps, keyGeneratorType,
					batchParams);
		}
		return 0;
	}

	protected void executeBatch(MappedStatement mappedStatement,
			PreparedStatement ps, KeyGeneratorType keyGenerator,
			List<Object> batchParams)
					throws SQLException {
		int[] batch = ps.executeBatch();
		BatchExecuteContext.addUpdatedRowCount(batch);

		if (keyGenerator == KeyGeneratorType.SELECT_AFTER) {

			String[] keyProperties = mappedStatement.getKeyProperties();
			if (keyProperties != null && keyProperties.length > 0) {
				// just one key property is supported
				String keyProperty = keyProperties[0];
				ResultSet keySet = ps.getGeneratedKeys();
				List<Object> keyList = getGeneratedKeys(keySet);

				Configuration configuration = mappedStatement
						.getConfiguration();

				for (int i = 0; i < keyList.size(); i++) {
					Object param = batchParams.get(i);
					Object key = keyList.get(i);
					setValue(keyProperty, configuration, param,
							key);
				}
			}
		}
	}

	protected void setValue(String keyProperty,
			Configuration configuration, Object param, Object key) {

		MetaObject metaParam = configuration
				.newMetaObject(param);

		Class<?> keyPropertyType = metaParam
				.getSetterType(keyProperty);

		if (keyPropertyType.isPrimitive()) {
			if (keyPropertyType.getName() == "int") {
				metaParam.setValue(keyProperty,
						((Number) key).intValue());
			} else {
				metaParam.setValue(keyProperty,
						((Number) key).longValue());
			}
		} else {
			if (keyPropertyType == Integer.class) {
				metaParam.setValue(keyProperty,
						((Number) key).intValue());
			} else if (keyPropertyType == Long.class) {
				metaParam.setValue(keyProperty,
						((Number) key).longValue());
			} else {
				metaParam.setValue(keyProperty,
						key);
			}
		}
	}

	// just one key property is supported
	private List<Object> getGeneratedKeys(ResultSet keySet)
			throws SQLException {
		List<Object> list = new ArrayList<Object>();
		while (keySet.next()) {
			Object key = keySet.getObject(1);
			list.add(key);
		}
		return list;
	}

	private void prepareBatchExecuteContext() {
		BatchExecuteContext.getUpdatedResult().clear();
	}

	protected int getAffectedRowCount() {
		int count = 0;
		List<Integer> list = BatchExecuteContext
				.getUpdatedResult();
		for (Integer i : list) {
			if (i > 0) {
				count += i;
			}
		}
		return count;
	}

	public Object plugin(Object target) {
		return Plugin.wrap(target, this);
	}

	public void setProperties(Properties properties) {
	}

}
