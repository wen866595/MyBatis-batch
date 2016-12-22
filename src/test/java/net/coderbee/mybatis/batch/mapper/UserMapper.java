package net.coderbee.mybatis.batch.mapper;

import java.util.List;

import net.coderbee.mybatis.batch.BatchParameter;
import net.coderbee.mybatis.batch.entity.User;

/**
 * @author coderbee 2016年12月21日 下午8:44:31
 *
 */
public interface UserMapper {

	List<User> selectAll();

	int deleteBatch(BatchParameter<User> users);

	int updateBatch(BatchParameter<User> users);

	int batchInsert2derby(BatchParameter<User> users);

	int batchInsert2Hsqldb(BatchParameter<User> users);

}
