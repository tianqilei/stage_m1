package query;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.hadoop.classification.InterfaceAudience.Private;

import tools.Millis2HHmmss;
import tools.TimeGettor;
import conn2database.Conn2Postgresql;

public class Query4Postgresql {

	// query 1 : tout le profil de user t.q id="usr1".
	// query 2 : l'âge de tous les utilisateurs de country : "Fr".
	// query 3 : le nombre de fois que chaque items a été acheté.
	// query 4 : les 10 items les plus vendus.
	// query 5 : les 10 items pas vendu du tout.
	// query 6 : les 100 pages les plus visitées
	// query 7 : les amis ayant acheté le même produit

	// query 8 : the product of highest price
	private Conn2Postgresql conn2Postgresql;
	private final static String ORDER_ID_4_QUERY9 = "order565";
	private final static String SHOP_ID_4_QUERY10 = "shop9992";
	private final static String SHOP_ID_4_QUERY11 = "shop9992";
	private final static String QUERY_PATH_POSTGRESQL = "./notes/query/query_postgresql.txt";

	private final static String QUERY1 = "SELECT usr.id_usr, name, sex, age, street, city, country FROM addr, usr where usr.id_usr=addr.id_usr AND usr.id_usr='usr5000';";
	private final static String QUERY2 = "SELECT age FROM addr, usr WHERE usr.id_usr=addr.id_usr AND country='tappets';";
	private final static String QUERY3 = "SELECT id_item, sum(quantity) AS nb FROM lineitem GROUP BY id_item;";
	private final static String QUERY4 = "SELECT id_item, count(*) AS nb FROM visit WHERE buy='y' GROUP BY id_item ORDER BY nb DESC LIMIT 10;";
	private final static String QUERY5 = "SELECT id_item, count(*) AS nb FROM visit WHERE buy='n' GROUP BY id_item ORDER BY nb DESC LIMIT 10;";
	private final static String QUERY6 = "SELECT url, count(*) AS nb FROM visit, item WHERE visit.id_item=item.id_item GROUP BY url ORDER BY nb DESC LIMIT 100;";
	private final static String QUERY7 = "SELECT DISTINCT id_usr1, id_usr2, v1.id_item FROM friend, usr u1, usr u2, visit v1, visit v2 WHERE id_usr1=u1.id_usr and id_usr2=u2.id_usr and u1.id_usr=v1.id_usr and u2.id_usr=v2.id_usr and v1.id_item=v2.id_item and v1.buy='y' and v2.buy='y';";

	private final static String QUERY8 = "SELECT max(price) FROM item";
	private final static String QUERY9 = "select total_price / sum(quantity) from ordered o, lineitem li where o.id_order=li.id_order and o.id_order='"
			+ ORDER_ID_4_QUERY9 + "' group by o.id_order";
	private final static String QUERY10 = "SELECT name,price FROM item WHERE id_shop='"
			+ SHOP_ID_4_QUERY10 + "'";
	private final static String QUERY11 = "SELECT id_shop, sum(price)/count(*) FROM item GROUP BY id_shop having id_shop='"
			+ SHOP_ID_4_QUERY11 + "'";

	public Query4Postgresql(String databaseName, String user, String password) {
		// TODO Auto-generated constructor stub
		conn2Postgresql = new Conn2Postgresql(databaseName, user, password);

		String queryString = QUERY1 + "\n" + QUERY2 + "\n" + QUERY3 + "\n"
				+ QUERY4 + "\n" + QUERY5 + "\n" + QUERY6 + "\n" + QUERY7;
		writeData(queryString);
	}

	public static void writeData(String text) {
		Writer writer = null;

		try {

			File file = new File(QUERY_PATH_POSTGRESQL);
			writer = new BufferedWriter(new FileWriter(file));
			writer.write(text);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				if (writer != null) {
					writer.close();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	public void runQuery1() throws SQLException {
		long timeBegin = TimeGettor.get();
		ResultSet result = conn2Postgresql.select(QUERY1);
		long timeEnd = TimeGettor.get();

		System.out.println("using time of running query 1 in Postgresql : "
				+ Millis2HHmmss.formatLongToTimeStr(timeEnd - timeBegin));
		/*
		 * while (result.next()) { System.out.println("ID : " +
		 * result.getString("id_usr")); System.out.println("Name : " +
		 * result.getString("name")); System.out.println("Sex : " +
		 * result.getString("sex")); System.out.println("Age : " +
		 * result.getString("age")); System.out.println("Address : ");
		 * System.out.println("  street : " + result.getString("street"));
		 * System.out.println("  city : " + result.getString("city"));
		 * System.out.println("  state : " + result.getString("country")); }
		 */
	}

	public void runQuery2() throws SQLException {
		long timeBegin = TimeGettor.get();
		ResultSet resultSet = conn2Postgresql.select(QUERY2);
		long timeEnd = TimeGettor.get();
		System.out.println("using time of running query 2 in Postgresql : "
				+ Millis2HHmmss.formatLongToTimeStr(timeEnd - timeBegin));
		/*
		 * while (resultSet.next()) { System.out.println("age : " +
		 * resultSet.getString("age")); }
		 */
	}

	public void runQuery3() throws SQLException {
		long timeBegin = TimeGettor.get();
		ResultSet resultSet = conn2Postgresql.select(QUERY3);
		long timeEnd = TimeGettor.get();
		System.out.println("using time of running query 3 in Postgresql : "
				+ Millis2HHmmss.formatLongToTimeStr(timeEnd - timeBegin));
		/*
		 * while (resultSet.next()) { System.out.println("Item ID : " +
		 * resultSet.getString("id_item") + " nb : " +
		 * resultSet.getString("nb")); }
		 */
	}

	public void runQuery4() throws SQLException {
		long timeBegin = TimeGettor.get();
		ResultSet resultSet = conn2Postgresql.select(QUERY4);
		long timeEnd = TimeGettor.get();
		System.out.println("using time of running query 4 in Postgresql : "
				+ Millis2HHmmss.formatLongToTimeStr(timeEnd - timeBegin));
		int i = 1;
		/*
		 * while (resultSet.next()) { System.out.println(i + ", Item ID : " +
		 * resultSet.getString("id_item") + " nb : " +
		 * resultSet.getString("nb")); i++; }
		 */
	}

	public void runQuery5() throws SQLException {
		long timeBegin = TimeGettor.get();
		ResultSet resultSet = conn2Postgresql.select(QUERY5);
		long timeEnd = TimeGettor.get();
		System.out.println("using time of running query 5 in Postgresql : "
				+ Millis2HHmmss.formatLongToTimeStr(timeEnd - timeBegin));

		/*
		 * int i = 1; while (resultSet.next()) { System.out.println(i +
		 * ", Item ID : " + resultSet.getString("id_item") + " nb : " +
		 * resultSet.getString("nb")); i++; }
		 */
	}

	public void runQuery6() throws SQLException {
		long timeBegin = TimeGettor.get();
		ResultSet resultSet = conn2Postgresql.select(QUERY6);
		long timeEnd = TimeGettor.get();
		System.out.println("using time of running query 6 in Postgresql : "
				+ Millis2HHmmss.formatLongToTimeStr(timeEnd - timeBegin));

		int i = 1;
		/*
		 * while (resultSet.next()) { System.out.println(i + ", url : " +
		 * resultSet.getString("url") + " nb : " + resultSet.getString("nb"));
		 * i++; }
		 */
	}

	public void runQuery7() throws SQLException {
		long timeBegin = TimeGettor.get();
		ResultSet resultSet = conn2Postgresql.select(QUERY7);
		long timeEnd = TimeGettor.get();
		System.out.println("using time of running query 7 in Postgresql : "
				+ Millis2HHmmss.formatLongToTimeStr(timeEnd - timeBegin));
		/*
		 * while (resultSet.next()) { System.out.println("Friend : " +
		 * resultSet.getString("id_usr1") + "," + resultSet.getString("id_usr2")
		 * + "; Item ID : " + resultSet.getString("id_item")); }
		 */

	}

	public void runQuery8() throws SQLException {
		long timeBegin = TimeGettor.get();
		ResultSet resultSet = conn2Postgresql.select(QUERY8);
		long timeEnd = TimeGettor.get();
		System.out.println("using time of running query 8 in Postgresql : "
				+ Millis2HHmmss.formatLongToTimeStr(timeEnd - timeBegin));
	}

	public void runQuery9() throws SQLException {
		long timeBegin = TimeGettor.get();
		ResultSet resultSet = conn2Postgresql.select(QUERY9);
		long timeEnd = TimeGettor.get();
		System.out.println("using time of running query 9 in Postgresql : "
				+ Millis2HHmmss.formatLongToTimeStr(timeEnd - timeBegin));
	}

	public void runQuery10() throws SQLException {
		long timeBegin = TimeGettor.get();
		ResultSet resultSet = conn2Postgresql.select(QUERY10);
		long timeEnd = TimeGettor.get();
		System.out.println("using time of running query 10 in Postgresql : "
				+ Millis2HHmmss.formatLongToTimeStr(timeEnd - timeBegin));
	}

	public void runQuery11() throws SQLException {
		long timeBegin = TimeGettor.get();
		ResultSet resultSet = conn2Postgresql.select(QUERY11);
		long timeEnd = TimeGettor.get();
		System.out.println("using time of running query 11 in Postgresql : "
				+ Millis2HHmmss.formatLongToTimeStr(timeEnd - timeBegin));
	}

	public static void main(String args[]) throws SQLException {
		Query4Postgresql query4Postgresql = new Query4Postgresql("my_data2",
				"postgres", "fenghuang");
		System.out.println("*** query 1 ***");
		// query4Postgresql.runQuery1();
		System.out.println("*** query 2 ***");
		// query4Postgresql.runQuery2();
		System.out.println("*** query 3 ***");
		// query4Postgresql.runQuery3();
		System.out.println("*** query 4 ***");
		// query4Postgresql.runQuery4();
		System.out.println("*** query 5 ***");
		// query4Postgresql.runQuery5();
		System.out.println("*** query 6 ***");
		// query4Postgresql.runQuery6();
		System.out.println("*** query 7 ***");
		// query4Postgresql.runQuery7();
		System.out.println("***query 8 ***");
		// query4Postgresql.runQuery8();
		System.out.println("*** query 9 ***");
		// query4Postgresql.runQuery9();
		System.out.println("*** query 10 ***");
		// query4Postgresql.runQuery10();
		System.out.println("*** query 11 ***");
		query4Postgresql.runQuery11();
	}
}
