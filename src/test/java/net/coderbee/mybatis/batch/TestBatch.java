package net.coderbee.mybatis.batch;

import java.io.Reader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.ibatis.io.Resources;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.ibatis.session.SqlSessionFactoryBuilder;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import net.coderbee.mybatis.batch.entity.User;
import net.coderbee.mybatis.batch.mapper.UserMapper;

/**
 * @author coderbee 2016年12月21日 下午9:49:51
 *
 */
public class TestBatch extends TestBatchBase {
	private static SqlSessionFactory derbySqlSessionFactory;
	private static SqlSessionFactory hsqldbSqlSessionFactory;

	@BeforeClass
	public static void setup() throws Exception {
		String resource = "configuration.xml";

		createDerbyDataSource();
		Reader reader = Resources.getResourceAsReader(resource);
		derbySqlSessionFactory = new SqlSessionFactoryBuilder().build(reader,
				"derby");

		createHsqldbDataSource();
		Reader hsqlReader = Resources.getResourceAsReader(resource);
		hsqldbSqlSessionFactory = new SqlSessionFactoryBuilder()
				.build(hsqlReader, "hsqldb");
	}

	@Test
	public void testOnDerby() {
		SqlSession sqlSession = derbySqlSessionFactory.openSession();

		UserMapper userMapper = sqlSession.getMapper(UserMapper.class);

		List<User> userList = buildUsers();

		BatchParameter<User> users = BatchParameter.wrap(userList);
		userMapper.batchInsert2derby(users);

		// 对于批量插入， derby 不支持返回最近生成的 ID，只能返回最近的一个
		// checkResult(userList);

		testBatchDelete(userMapper);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testOnHsqldb() {
		SqlSession sqlSession = hsqldbSqlSessionFactory.openSession();

		UserMapper userMapper = sqlSession.getMapper(UserMapper.class);

		List<User> userList = buildUsers();

		BatchParameter<User> users = BatchParameter.wrap(userList);
		userMapper.batchInsert2Hsqldb(users);

		checkResult(userList);

		testBatchDelete(userMapper);

		Map<String, Object> map = new HashMap<String, Object>();
		map.put("name", "abc123");
		map.put("email", "abc123@126.com");
		List<Map<String, Object>> list = Arrays.asList(map);
		userMapper.batchInsertByMap(
				BatchParameter.<Map<String, Object>> wrap(list));

		Assert.assertNotNull(((Number) map.get("id")).intValue() == 1003);
	}

	public void testBatchDelete(UserMapper userMapper) {
		List<User> list = userMapper.selectAll();

		List<User> users = buildToUpdateUsers();
		int counts = userMapper.deleteBatch(BatchParameter.wrap(users));
		Assert.assertTrue(counts == 2);

		List<User> afterDeleteList = userMapper.selectAll();
		Assert.assertTrue(list.size() - afterDeleteList.size() == 2);
	}

	protected List<User> buildToUpdateUsers() {
		List<User> users = new ArrayList<User>();
		users.add(createUser(1, "-x"));
		users.add(createUser(2, "-y"));
		users.add(createUser(3, "-z"));
		return users;
	}

	protected void checkResult(List<User> userList) {
		int idStart = 1000; // 自动增长的 ID 从 1000 开始
		for (User user : userList) {
			Assert.assertEquals(idStart++, user.getId());
		}
	}

	protected List<User> buildUsers() {
		List<User> userList = new ArrayList<User>();
		userList.add(createUser("bruce.liu", "wen866595@163.com"));
		userList.add(createUser("coderbee.net", "wen866595@gmail.com"));
		userList.add(createUser("world", "hello@gmail.com"));
		return userList;
	}

	private User createUser(int id, String name) {
		User u = new User();
		u.setId(id);
		u.setName(name);
		return u;
	}

	private User createUser(String name, String email) {
		User u = new User();
		u.setName(name);
		u.setEmail(email);
		return u;
	}

}
