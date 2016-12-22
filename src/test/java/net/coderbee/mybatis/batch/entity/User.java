package net.coderbee.mybatis.batch.entity;

import java.io.Serializable;

/**
 * @author coderbee 2016年12月21日 下午8:46:39
 *
 */
@SuppressWarnings("serial")
public class User implements Serializable {
	private int id;
	private String name;
	private String email;

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getEmail() {
		return email;
	}

	public void setEmail(String email) {
		this.email = email;
	}

	public String toString() {
		return "id:" + id + ", name:" + name + ", email:" + email;
	}

}
