package net.coderbee.mybatis.batch;

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

	public static void addUpdatedRowCount(int[] counts) {
		List<Integer> list = updatedCount.get();
		for (int i : counts) {
			list.add(i);
		}
	}

	public static List<Integer> getUpdatedResult() {
		return updatedCount.get();
	}

}
