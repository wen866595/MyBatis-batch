package net.coderbee.mybatis.batch;

import java.sql.PreparedStatement;
import java.util.Properties;

import org.apache.ibatis.executor.parameter.DefaultParameterHandler;
import org.apache.ibatis.executor.parameter.ParameterHandler;
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

			Object paramObj = ReflectHelper
					.getValueByFieldName(parameterHandler, "parameterObject");
			if (paramObj instanceof BatchParameter) {
				// 不绑定参数，在执行阶段才绑定
				return null;
			}
		}

		return invocation.proceed();
	}

	public Object plugin(Object target) {
		return Plugin.wrap(target, this);
	}

	public void setProperties(Properties properties) {
	}

}
