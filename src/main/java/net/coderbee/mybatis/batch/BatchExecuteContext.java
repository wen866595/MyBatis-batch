package net.coderbee.mybatis.batch;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;

/**
 * 批量执行的上下文，用于持有 受影响的行数、自动生成的主键。
 * 
 * @author <a href="http://coderbee.net">coderbee</a>
 */
public class BatchExecuteContext {
	/**
	 * 受影响的行数
	 */
	private static ThreadLocal<List<Integer>> updatedCount = new ThreadLocal<List<Integer>>() {
		@Override
		protected List<Integer> initialValue() {
			return new LinkedList<Integer>();
		}
	};

	/**
	 * 执行 insert 语句时自动生成的主键
	 */
	private static ThreadLocal<List<Object>> generatedKeys = new ThreadLocal<List<Object>>() {
		@Override
		protected List<Object> initialValue() {
			return new LinkedList<Object>();
		}
	};

	public static void addUpdatedRowCount(int[] counts, int maxSize) {
		List<Integer> list = updatedCount.get();
		for (int i : counts) {
			list.add(i);
		}
	}

	public static void addGeneratedKeys(ResultSet keySet, int maxSize)
			throws SQLException {
		List<Object> list = generatedKeys.get();
		int columnCount = keySet.getMetaData().getColumnCount();
		while (keySet.next()) {
			if (columnCount > 1) {
				Object[] keys = getKeys(keySet, columnCount);
				list.add(keys);
			} else {
				Object key = keySet.getObject(1);
				list.add(key);
			}
		}
	}

	/**
	 * 获取复合主键
	 * 
	 * @param keySet
	 * @param columnCount
	 * @return
	 * @throws SQLException
	 */
	private static Object[] getKeys(ResultSet keySet, int columnCount)
			throws SQLException {
		Object[] keys = new Object[columnCount];
		for (int i = 1; i <= columnCount; i++) {
			keys[i - 1] = keySet.getObject(i);
		}
		return keys;
	}

	public static List<Integer> getUpdatedResult() {
		return updatedCount.get();
	}

	public static List<Object> getGeneratedKeys() {
		return generatedKeys.get();
	}

}
