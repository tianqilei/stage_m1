package conn2database;

import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;

/*
 * This class is for connection postgresql by using JDBC
 * */
public class Conn2Postgresql {

	private String nom_db;
	private String user;
	private String password;
	private Statement sate;
	private Connection conn;

	public Conn2Postgresql(String nom_db, String user, String password) {
		this.nom_db = nom_db;
		this.user = user;
		this.password = password;
		try {
			Class.forName("org.postgresql.Driver").newInstance();
			String url = "jdbc:postgresql://localhost:5432/".concat(nom_db);
			conn = DriverManager.getConnection(url, user, password);
			sate = conn.createStatement();

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/*
	 * sql should be a sql "select ..."
	 */
	public ResultSet select(String sql) {
		ResultSet getResult = null;
		try {
			getResult = sate.executeQuery(sql);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return getResult;
	}

	public void copyFromFile(String fileName, String tableName)
			throws SQLException, IOException {
		CopyManager copyManager = new CopyManager((BaseConnection) conn);
		FileReader fileReader = new FileReader(fileName);

		copyManager.copyIn("COPY " + tableName
				+ " FROM STDIN WITH DELIMITER ';'", fileReader);
	}

	public void traiterResultat(ResultSet getResult) {
		// String strSql = "select * from requete";

		try {
			while (getResult.next()) {
				System.out.println("**********************");
				System.out.println(getResult.getString(1));
				System.out.println(getResult.getString(2));
				System.out.println(getResult.getString(3));
				System.out.println("**********************");
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/*
	 * sql should be a sql "insert ..."
	 */
	public void insert(String sql) {
		try {
			sate.execute(sql);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void deleteAllData() {
		try {
			sate.execute("delete from ordered");
			sate.execute("delete from lineitem");
			sate.execute("delete from visit");
			sate.execute("delete from addr");
			sate.execute("delete from usr");
			sate.execute("delete from item");
			sate.execute("delete from shop");
			sate.execute("delete from ordered");
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public String getNom_db() {
		return nom_db;
	}

	public void setNom_db(String nom_db) {
		this.nom_db = nom_db;
	}

	public String getUser() {
		return user;
	}

	public void setUser(String user) {
		this.user = user;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public Statement getSate() {
		return sate;
	}

	public void setSate(Statement sate) {
		this.sate = sate;
	}

	public static void main(String[] args) throws Exception {
		Conn2Postgresql conn2Postgresql = new Conn2Postgresql("my_data",
				"postgres", "fenghuang");
		conn2Postgresql.deleteAllData();
		conn2Postgresql.copyFromFile("./data/users.txt", "usr");

		System.out.println("done");

	}
}
