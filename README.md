# MyBatis-batch
基于 MyBatis 执行 SQL 批量操作的插件

MyBatis conf 配置：
```xml
<configuration>
	<plugins>
		<plugin interceptor="net.coderbee.mybatis.batch.BatchParameterHandler" />
		<plugin interceptor="net.coderbee.mybatis.batch.BatchStatementHandler" />
	</plugins>
</configuration>
```

Mapper XML 编写说明，需要把 `parameterType` 指定为 `net.coderbee.mybatis.batch.BatchParameter`，如下：
```xml
<insert id="addBatch" parameterType="net.coderbee.mybatis.batch.BatchParameter">
	insert into
	auto_increment_id(name) values (#{name})
</insert>
```
调用示例如下：
```java
@Test
public void testBatchInsert() {
	List<Tester> params = buildParams();

  // 执行	批量操作，返回受影响的行数之和
  int cnt = mapper.addBatch(BatchParameter.wrap(params));
	System.err.println(cnt);

  // 如果指定了获取自动生成的主键 `useGeneratedKeys="true"` ，可通过下面的方式获取。否则拿到的是空列表。
  List<Object> keys = BatchExecuteContext.getGeneratedKeys();
	System.err.println(keys);
}

protected List<Tester> buildParams() {
	List<Tester> params = new ArrayList<>();
	Tester t1 = new Tester();
	t1.setName("t1");
	params.add(t1);

	Tester t2 = new Tester();
	t2.setName("t2");
	params.add(t2);
	return params;
}
```

