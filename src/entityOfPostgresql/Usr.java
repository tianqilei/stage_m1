package entityOfPostgresql;

public class Usr {

	private String id_usr;
	private String name;
	private char sex;
	private int age;

	public Usr(String id_usr, String name, char sex, int age) {
		super();
		this.id_usr = id_usr;
		this.name = name;
		this.sex = sex;
		this.age = age;
	}

	public String getId_usr() {
		return id_usr;
	}

	public void setId_usr(String id_usr) {
		this.id_usr = id_usr;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public char getSex() {
		return sex;
	}

	public void setSex(char sex) {
		this.sex = sex;
	}

	public int getAge() {
		return age;
	}

	public void setAge(int age) {
		this.age = age;
	}

	@Override
	public String toString() {
		return "Usr [id_usr=" + id_usr + ", name=" + name + ", sex=" + sex
				+ ", age=" + age + "]";
	}

	public String toOutputString(String seperator, String nextLine) {
		return getId_usr() + seperator + getName() + seperator + getSex()
				+ seperator + getAge() + nextLine;
	}
}
