package net.coderbee.mybatis.batch;

import java.sql.Statement;
import java.util.List;
import java.util.Properties;

import org.apache.ibatis.executor.statement.PreparedStatementHandler;
import org.apache.ibatis.executor.statement.RoutingStatementHandler;
import org.apache.ibatis.executor.statement.StatementHandler;
import org.apache.ibatis.mapping.BoundSql;
import org.apache.ibatis.plugin.Interceptor;
import org.apache.ibatis.plugin.Intercepts;
import org.apache.ibatis.plugin.Invocation;
import org.apache.ibatis.plugin.Plugin;
import org.apache.ibatis.plugin.Signature;

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

	public Object intercept(Invocation invocation) throws Throwable {
		if (invocation.getTarget() instanceof RoutingStatementHandler) {
			RoutingStatementHandler routingStatementHandler = (RoutingStatementHandler) invocation
					.getTarget();

			StatementHandler delegate = (StatementHandler) ReflectHelper
					.getValueByFieldName(routingStatementHandler, "delegate");
			if (delegate instanceof PreparedStatementHandler) {

				PreparedStatementHandler preparedStatementHandler = (PreparedStatementHandler) delegate;

				BoundSql boundSql = (BoundSql) ReflectHelper
						.getValueByFieldName(preparedStatementHandler,
								"boundSql");

				Object parameterObject = boundSql.getParameterObject();
				if (parameterObject instanceof BatchParameter) {
					int count = getAffectedRowCount();
					return count;
				}
			}
		}

		return invocation.proceed();
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
