package net.coderbee.mybatis.batch;

import java.util.ArrayList;
import java.util.List;

/**
 * 封装要批量操作的参数，插件判断 Mapper 的 parameterType 是此类的实例时才走批量插入， 否则按 MyBatis 的默认方式处理。
 * 
 * @param <T>
 *            要插入记录的实体类型
 * 
 * @author <a href="http://coderbee.net">coderbee</a>
 *
 */
public class BatchParameter<T> {
	private static final int DEFAULT_BATCH_SIZE = 1000;

	private final List<T> data;
	private final int batchSize;
	private final List<Integer> affectedRowCounts;

	private BatchParameter(List<T> data, int batchSize) {
		this.data = data;
		this.batchSize = batchSize;
		affectedRowCounts = new ArrayList<Integer>(data.size());
	}

	public List<T> getData() {
		return data;
	}

	public int getBatchSize() {
		return batchSize;
	}

	public int getAffectedRowCount() {
		int count = 0;
		for (Integer i : affectedRowCounts) {
			if (i > 0) {
				count += i;
			}
		}
		return count;
	}

	public void addRowCounts(int[] counts) {
		for (int i : counts) {
			affectedRowCounts.add(i);
		}
	}

	public static <T> BatchParameter<T> wrap(List<T> data) {
		return wrap(data, DEFAULT_BATCH_SIZE);
	}

	/**
	 * 
	 * @param data
	 *            不能是空
	 * @param batchSize
	 *            不能小于等于 0
	 * @return
	 */
	public static <T> BatchParameter<T> wrap(List<T> data, int batchSize) {
		if (data == null || data.isEmpty()) {
			throw new IllegalArgumentException("数据为空");
		}

		if (batchSize < 1) {
			throw new IllegalArgumentException("batchSize < 1");
		}

		return new BatchParameter<T>(data, batchSize);
	}
}
