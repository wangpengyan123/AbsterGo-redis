package test;


import java.io.Serializable;
@SuppressWarnings("serial")
public class AppUser implements Serializable{
	
	
	private String userId;
	public String getUserId() {
		return userId;
	}
	public void setUserId(String userId) {
		this.userId = userId;
	}
	private String name;
	private String age;
	private String pwd;
	public String getName() {
		return name;
	}
	public AppUser()
	{
		
	}
	public AppUser(String name, String age, String pwd) {
		super();
		this.name = name;
		this.age = age;
		this.pwd = pwd;
	}
	public void setName(String name) {
		this.name = name;
	}
	public String getAge() {
		return age;
	}
	public void setAge(String age) {
		this.age = age;
	}
	public String getPwd() {
		return pwd;
	}
	public void setPwd(String pwd) {
		this.pwd = pwd;
	}
	
}
