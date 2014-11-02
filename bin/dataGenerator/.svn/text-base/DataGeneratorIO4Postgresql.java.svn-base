package dataGenerator;

import java.io.IOException;
import java.sql.SQLException;

import tools.Millis2HHmmss;
import tools.TimeGettor;
import conn2database.Conn2Postgresql;
import entityOfPostgresql.Address;
import entityOfPostgresql.Friend;
import entityOfPostgresql.Item;
import entityOfPostgresql.LineItem;
import entityOfPostgresql.Ordered;
import entityOfPostgresql.Shop;
import entityOfPostgresql.Usr;
import entityOfPostgresql.Visit;

/*
 * This class is for writing the data into Postgresql
 * */

public class DataGeneratorIO4Postgresql {

	public static void copyUsers(Conn2Postgresql conn2Postgresql) {
		String path = DataGeneratorIO.PATH_DIR + DataGeneratorIO.PATH_USR;
		try {
			long timeBegin = TimeGettor.get();
			conn2Postgresql.copyFromFile(path, "usr");
			long timeEnd = TimeGettor.get();
			System.out.println("time : "
					+ Millis2HHmmss.formatLongToTimeStr(timeEnd - timeBegin));
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static void copyShop(Conn2Postgresql conn2Postgresql) {
		String path = DataGeneratorIO.PATH_DIR + DataGeneratorIO.PATH_SHOP;
		try {
			long timeBegin = TimeGettor.get();
			conn2Postgresql.copyFromFile(path, "shop");
			long timeEnd = TimeGettor.get();
			System.out.println("time : "
					+ Millis2HHmmss.formatLongToTimeStr(timeEnd - timeBegin));
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public static void copyItem(Conn2Postgresql conn2Postgresql) {
		String path = DataGeneratorIO.PATH_DIR + DataGeneratorIO.PATH_ITEM;
		try {
			long timeBegin = TimeGettor.get();
			conn2Postgresql.copyFromFile(path, "item");
			long timeEnd = TimeGettor.get();
			System.out.println("time : "
					+ Millis2HHmmss.formatLongToTimeStr(timeEnd - timeBegin));
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static void copyAddress(Conn2Postgresql conn2Postgresql) {
		String path = DataGeneratorIO.PATH_DIR + DataGeneratorIO.PATH_ADDR;
		try {
			long timeBegin = TimeGettor.get();
			conn2Postgresql.copyFromFile(path, "addr");
			long timeEnd = TimeGettor.get();
			System.out.println("time : "
					+ Millis2HHmmss.formatLongToTimeStr(timeEnd - timeBegin));
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static void copyFriend(Conn2Postgresql conn2Postgresql) {
		String path = DataGeneratorIO.PATH_DIR + DataGeneratorIO.PATH_FRIENDS;
		try {
			long timeBegin = TimeGettor.get();
			conn2Postgresql.copyFromFile(path, "friend");
			long timeEnd = TimeGettor.get();
			System.out.println("time : "
					+ Millis2HHmmss.formatLongToTimeStr(timeEnd - timeBegin));
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static void copyOrder(Conn2Postgresql conn2Postgresql) {
		String path = DataGeneratorIO.PATH_DIR + DataGeneratorIO.PATH_ORDER;
		try {
			long timeBegin = TimeGettor.get();
			conn2Postgresql.copyFromFile(path, "ordered");
			long timeEnd = TimeGettor.get();
			System.out.println("time : "
					+ Millis2HHmmss.formatLongToTimeStr(timeEnd - timeBegin));
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static void copyVisit(Conn2Postgresql conn2Postgresql) {
		String path = DataGeneratorIO.PATH_DIR + DataGeneratorIO.PATH_VISIT;
		try {
			long timeBegin = TimeGettor.get();
			conn2Postgresql.copyFromFile(path, "visit");
			long timeEnd = TimeGettor.get();
			System.out.println("time : "
					+ Millis2HHmmss.formatLongToTimeStr(timeEnd - timeBegin));
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static void copyLineItem(Conn2Postgresql conn2Postgresql) {
		String path = DataGeneratorIO.PATH_DIR + DataGeneratorIO.PATH_LINEITEM;
		try {
			long timeBegin = TimeGettor.get();
			conn2Postgresql.copyFromFile(path, "lineitem");
			long timeEnd = TimeGettor.get();
			System.out.println("time : "
					+ Millis2HHmmss.formatLongToTimeStr(timeEnd - timeBegin));
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static void writeShop(Shop shop, Conn2Postgresql conn2Postgresql) {
		String sql = "INSERT INTO shop VALUES ('" + shop.getId_shop() + "', '"
				+ shop.getName() + "', '" + shop.getUrl() + "');";
		// System.out.println(sql);
		conn2Postgresql.insert(sql);
	}

	public static void writeItem(Item item, Conn2Postgresql conn2Postgresql) {
		String sql = "INSERT INTO item VALUES ('" + item.getId_item() + "','"
				+ item.getName() + "', " + item.getPrice() + ", '"
				+ item.getUrl() + "', '" + item.getDescription() + "')";
		conn2Postgresql.insert(sql);
	}

	public static void writeUsr(Usr usr, Conn2Postgresql conn2Postgresql) {
		String sql = "INSERT INTO usr VALUES ('" + usr.getId_usr() + "', '"
				+ usr.getName() + "', '" + usr.getSex() + "', " + usr.getAge()
				+ ");";
		conn2Postgresql.insert(sql);
	}

	public static void writeAddress(Address address,
			Conn2Postgresql conn2Postgresql) {
		String sql = "INSERT INTO addr VALUES ('" + address.getStreet()
				+ "', '" + address.getCity() + "', '" + address.getCountry()
				+ "', '" + address.getIdUsr() + "')";
		conn2Postgresql.insert(sql);
	}

	public static void writeFriend(Friend friend,
			Conn2Postgresql conn2Postgresql) {
		String sql = "INSERT INTO friend VALUES ('" + friend.getIdUsr1()
				+ "', '" + friend.getIdUsr2() + "')";
		conn2Postgresql.insert(sql);
	}

	public static void writeOrder(Ordered ordered,
			Conn2Postgresql conn2Postgresql) {
		String sql = "INSERT INTO ordered VALUES ('" + ordered.getIdOrder()
				+ "', '" + ordered.getIdUsr() + "', '" + ordered.getIdShop()
				+ "', '" + ordered.getYear() + "-" + ordered.getMonth() + "-"
				+ ordered.getDay() + "', " + ordered.getTotalPrice() + ")";
		conn2Postgresql.insert(sql);
	}

	public static void writeVisit(Visit visit, Conn2Postgresql conn2Postgresql) {
		String sql = "INSERT INTO visit VALUES ('" + visit.getIdUsr() + "', '"
				+ visit.getIdItem() + "', '" + visit.getYear() + "-"
				+ visit.getMonth() + "-" + visit.getDay() + "', '"
				+ visit.getHour() + ":" + visit.getMinute() + ":"
				+ visit.getSecond() + "', '" + visit.getBuy() + "');";
		conn2Postgresql.insert(sql);
	}

	public static void writeLineItem(LineItem lineItem,
			Conn2Postgresql conn2Postgresql) {
		String sql = "INSERT INTO lineitem VALUES ('" + lineItem.getOrderId()
				+ "', '" + lineItem.getItemId() + "'," + lineItem.getQuantity()
				+ ")";
		conn2Postgresql.insert(sql);
	}
}
