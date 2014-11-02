package dataGenerator;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.File;
import java.io.Writer;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;

import tools.GetIntFromId;
import tools.Millis2HHmmss;
import tools.TimeGettor;
import tools.TransformInteger;
import avro.Addresses;
import avro.Friendship;
import avro.ItemLine;
import avro.ItemLines;
import avro.Items;
import avro.OrderQuantity;
import avro.OrderQuantitys;
import avro.UsrBuy;
import avro.Visits;
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
 * I/O of data generator 
 * load data into posgresql
 * 
 * default directory of output is ./data/
 * default output file is the file of ".txt"
 * default data seperator is ";"
 * default time seperator is ":"
 * default "next line" string is "\n"
 * 
 * 
 * ### all method who write data into HBase must be run after storing into Postgresql ###
 * */
public class DataGeneratorIO {

	public static final int N = 1000;
	public static final int NB_USR = N;
	public static final int NB_SHOP = N;
	public static final int NB_ITEM = N;
	public static final int NB_ADDR = N;
	public static final int NB_FRIENDS = 5 * N;
	public static final int NB_VISIT = 10 * N;
	public static final int NB_ORDER = 10 * N;
	public static final int NB_LINEITEM = 10 * N;

	private static final int NB_TUPLE_WRITE_ONCE = 10000;

	private static final String EXTERNAL_NAME = ".txt";
	public static final String DATA_SEPERATOR = ";";
	private static final String DATE_DEPERATOR = "-";
	private static final String TIME_SEPERATOR = ":";
	private static final String NEXT_LINE = "\n";

	public static final String PATH_DIR = "./data/";
	public static final String PATH_SHOP = "shops" + EXTERNAL_NAME;
	public static final String PATH_USR = "users" + EXTERNAL_NAME;
	public static final String PATH_ITEM = "items" + EXTERNAL_NAME;
	public static final String PATH_ADDR = "address" + EXTERNAL_NAME;
	public static final String PATH_FRIENDS = "friends" + EXTERNAL_NAME;
	public static final String PATH_VISIT = "visit" + EXTERNAL_NAME;
	public static final String PATH_ORDER = "order" + EXTERNAL_NAME;
	public static final String PATH_LINEITEM = "lineitem" + EXTERNAL_NAME;

	private DataGenerator dGenerator;
	private Conn2Postgresql conn2Postgresql;
	private DataGeneraorIO4Hbase dataGeneraorIO4Hbase;
	private DataGeneratorIO4OracleNosql dataGeneratorIO4OracleNosql;

	private static final boolean write2postgresql = true; /* it must be true */
	private boolean write2hbase = true;
	private boolean write2files = true;
	private boolean write2OracleNosql = true;

	private HashMap<String, ArrayList<avro.Usr>> countryUsrsHashMap = new HashMap<String, ArrayList<avro.Usr>>();
	private HashMap<String, ArrayList<avro.UsrBuy>> itemUsrsHashMap = new HashMap<String, ArrayList<avro.UsrBuy>>();
	private HashMap<String, ArrayList<avro.OrderQuantity>> orderQuantityHashMap = new HashMap<String, ArrayList<avro.OrderQuantity>>();

	public DataGeneratorIO(String databaseName, String user, String password)
			throws Exception {
		dGenerator = new DataGenerator();
		conn2Postgresql = new Conn2Postgresql(databaseName, user, password);
		if (write2hbase) {
			dataGeneraorIO4Hbase = new DataGeneraorIO4Hbase();
		}
		if (write2OracleNosql) {
			dataGeneratorIO4OracleNosql = new DataGeneratorIO4OracleNosql();
		}
	}

	public void initData() throws Exception {
		if (write2postgresql) {
			conn2Postgresql.deleteAllData();
		}
		if (write2hbase) {
			dataGeneraorIO4Hbase.init();
		}

		File file = new File(PATH_DIR + PATH_ADDR);
		if (file.exists()) {
			while (file.delete() == false) {
				System.out.println("delete file " + PATH_ADDR
						+ " failed, try again ...");
			}
			System.out.println("File : " + PATH_ADDR + " deleted.");
		}

		file = new File(PATH_DIR + PATH_FRIENDS);
		if (file.exists()) {
			while (file.delete() == false) {
				System.out.println("delete file " + PATH_FRIENDS
						+ " failed, try again ...");
			}
			System.out.println("File : " + PATH_FRIENDS + " deleted.");
		}

		file = new File(PATH_DIR + PATH_ITEM);
		if (file.exists()) {
			while (file.delete() == false) {
				System.out.println("delete file " + PATH_ITEM
						+ " failed, try again ...");
			}
			System.out.println("File : " + PATH_ITEM + " deleted.");
		}

		file = new File(PATH_DIR + PATH_LINEITEM);
		if (file.exists()) {
			while (file.delete() == false) {
				System.out.println("delete file " + PATH_LINEITEM
						+ " failed, try again ...");
			}
			System.out.println("File : " + PATH_LINEITEM + " deleted.");
		}

		file = new File(PATH_DIR + PATH_ORDER);
		if (file.exists()) {
			while (file.delete() == false) {
				System.out.println("delete file " + PATH_ORDER
						+ " failed, try again ...");
			}
			System.out.println("File : " + PATH_ORDER + " deleted.");
		}

		file = new File(PATH_DIR + PATH_SHOP);
		if (file.exists()) {
			while (file.delete() == false) {
				System.out.println("delete file " + PATH_SHOP
						+ " failed, try again ...");
			}
			System.out.println("File : " + PATH_SHOP + " deleted.");
		}

		file = new File(PATH_DIR + PATH_USR);
		if (file.exists()) {
			while (file.delete() == false) {
				System.out.println("delete file " + PATH_USR
						+ " failed, try again ...");
			}
			System.out.println("File : " + PATH_USR + " deleted.");
		}

		file = new File(PATH_DIR + PATH_VISIT);
		if (file.exists()) {
			while (file.delete() == false) {
				System.out.println("delete file " + PATH_VISIT
						+ " failed, try again ...");
			}
			System.out.println("File : " + PATH_VISIT + " deleted.");
		}
		System.out.println("All tables initialized");
	}

	public static int getN() {
		return N;
	}

	public static void writeData(String text, String path) {
		Writer writer = null;
		try {
			File file = new File(PATH_DIR + path);
			writer = new BufferedWriter(new FileWriter(file, true));
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

	public void writeShops2Postgresql() throws Exception {
		DataGeneratorIO4Postgresql.copyShop(conn2Postgresql);
	}

	public void writeShops2HBase() throws Exception {
		ArrayList<Shop> shops = dGenerator.getShopList();
		HashMap<String, ArrayList<Item>> shopItems = dGenerator.getShopItems();
		long accuSec = (long) 0;
		for (int i = 0; i < shops.size(); i++) {
			Shop shop = shops.get(i);
			ArrayList<Item> itemList = shopItems.get(shop.getId_shop());
			long timeBegin = TimeGettor.get();
			dataGeneraorIO4Hbase.writeShop(shop, itemList);
			long timeEnd = TimeGettor.get();
			accuSec += (timeEnd - timeBegin);
		}
		System.out.println("using time of loading shops data into HBase: "
				+ Millis2HHmmss.formatLongToTimeStr(accuSec));
	}

	public void generateData() {
		long timeBegin = TimeGettor.get();
		String usersTextString = "";
		int compt = 0;
		for (int i = 0; i < NB_USR; i++) {
			Usr usr = dGenerator.getUsr();
			if (write2files) {
				compt++;
				usersTextString = usersTextString.concat(usr.toOutputString(
						DATA_SEPERATOR, NEXT_LINE));
				if (compt == NB_TUPLE_WRITE_ONCE) {
					DataGeneratorIO.writeData(usersTextString, PATH_USR);
					compt = 0;
					usersTextString = "";
				}
			}

		}
		if (write2files && !usersTextString.equals("")) {
			DataGeneratorIO.writeData(usersTextString, PATH_USR);
		}

		System.out.println("writing users data finished ...");
		compt = 0;

		String shopsText = "";
		for (int i = 0; i < NB_SHOP; i++) {
			Shop shop = dGenerator.getShop();
			if (write2files) {
				compt++;
				shopsText = shopsText.concat(shop.toOutputString(
						DATA_SEPERATOR, NEXT_LINE));
				if (compt == NB_TUPLE_WRITE_ONCE) {
					DataGeneratorIO.writeData(shopsText, PATH_SHOP);
					compt = 0;
					shopsText = "";
				}
			}
		}

		if (write2files && !shopsText.equals("")) {
			DataGeneratorIO.writeData(shopsText, PATH_SHOP);
		}

		System.out.println("writing shops data finished ...");
		compt = 0;

		String itemsTextString = "";
		for (int i = 0; i < NB_ITEM; i++) {
			Item item = dGenerator.getItem();

			if (write2files) {
				compt++;
				itemsTextString = itemsTextString.concat(item.toOutputString(
						DATA_SEPERATOR, NEXT_LINE));
				if (compt == NB_TUPLE_WRITE_ONCE) {
					DataGeneratorIO.writeData(itemsTextString, PATH_ITEM);
					compt = 0;
					itemsTextString = "";
				}
			}

		}

		if (write2files && !itemsTextString.equals("")) {
			DataGeneratorIO.writeData(itemsTextString, PATH_ITEM);
		}

		System.out.println("writing items data finished ...");
		compt = 0;

		String friendsTextString = "";
		for (int i = 0; i < NB_FRIENDS; i++) {
			// System.out.println("friend : " + i);
			Friend friend = dGenerator.getFriend();
			if (write2files) {
				compt++;
				friendsTextString = friendsTextString.concat(friend
						.toOutputString(DATA_SEPERATOR, NEXT_LINE));
				if (compt == NB_TUPLE_WRITE_ONCE) {
					DataGeneratorIO.writeData(friendsTextString, PATH_FRIENDS);
					compt = 0;
					friendsTextString = "";
				}
			}
		}

		if (write2files && !friendsTextString.equals("")) {
			DataGeneratorIO.writeData(friendsTextString, PATH_FRIENDS);
		}

		System.out.println("writing friends data finished ...");
		compt = 0;
		String addressTextString = "";
		for (int i = 0; i < NB_ADDR; i++) {
			Address address = dGenerator.getAddress();
			if (write2files) {
				compt++;
				addressTextString = addressTextString.concat(address
						.toOutputString(DATA_SEPERATOR, NEXT_LINE));
				if (compt == NB_TUPLE_WRITE_ONCE) {
					DataGeneratorIO.writeData(addressTextString, PATH_ADDR);
					compt = 0;
					addressTextString = "";
				}
			}
		}

		if (write2files && !addressTextString.equals("")) {
			DataGeneratorIO.writeData(addressTextString, PATH_ADDR);
		}

		System.out.println("writing adresses data finished ...");
		compt = 0;
		String orderTextString = "";
		for (int i = 0; i < NB_ORDER; i++) {
			Ordered ordered = dGenerator.getOrdered();
			if (write2files) {
				compt++;
				orderTextString = orderTextString.concat(ordered
						.toOutputString(DATA_SEPERATOR, NEXT_LINE,
								DATE_DEPERATOR));
				if (compt == NB_TUPLE_WRITE_ONCE) {
					DataGeneratorIO.writeData(orderTextString, PATH_ORDER);
					compt = 0;
					orderTextString = "";
				}
			}
		}
		if (write2files && !orderTextString.equals("")) {
			DataGeneratorIO.writeData(orderTextString, PATH_ORDER);
		}

		System.out.println("writing orders data finishd ...");
		compt = 0;
		String lineItemTextString = "";
		for (int i = 0; i < NB_LINEITEM; i++) {
			// System.out.println("itemine : " + i);
			LineItem lineItem = dGenerator.getLineItem();

			if (write2files) {
				compt++;
				lineItemTextString = lineItemTextString.concat(lineItem
						.toOutputString(DATA_SEPERATOR, NEXT_LINE));
				if (compt == NB_TUPLE_WRITE_ONCE) {
					DataGeneratorIO
							.writeData(lineItemTextString, PATH_LINEITEM);
					compt = 0;
					lineItemTextString = "";
				}
			}
		}
		if (write2files && !lineItemTextString.equals("")) {
			DataGeneratorIO.writeData(lineItemTextString, PATH_LINEITEM);
		}

		System.out.println("writing lineitem data finished ...");
		compt = 0;
		String visitTextString = "";
		for (int i = 0; i < NB_VISIT; i++) {
			// System.out.println("visit : " + i);
			Visit visit = dGenerator.getVisit();
			if (write2files) {
				compt++;
				visitTextString = visitTextString.concat(visit.toOutputString(
						DATA_SEPERATOR, NEXT_LINE, DATE_DEPERATOR,
						TIME_SEPERATOR));
				if (compt == NB_TUPLE_WRITE_ONCE) {
					DataGeneratorIO.writeData(visitTextString, PATH_VISIT);
					compt = 0;
					visitTextString = "";
				}
			}
		}
		if (write2files && !visitTextString.equals("")) {
			DataGeneratorIO.writeData(visitTextString, PATH_VISIT);
		}
		System.out.println("writing visits data finished ...");

		long timeEnd = TimeGettor.get();
		System.out.println("using time of generating data: "
				+ Millis2HHmmss.formatLongToTimeStr(timeEnd - timeBegin));
	}

	public void writeUsers2Postgresql() throws IOException {
		DataGeneratorIO4Postgresql.copyUsers(conn2Postgresql);
	}

	public void writeUsers2HBase() throws Exception {
		ArrayList<Usr> usrList = dGenerator.getUsrList();
		HashMap<String, ArrayList<Address>> addressHashMap = dGenerator
				.getAddressMap();

		long accuSec = (long) 0;
		for (int i = 0; i < usrList.size(); i++) {
			Usr usr = usrList.get(i);

			ArrayList<Address> addrs = null;

			if (addressHashMap.containsKey(usr.getId_usr())) {
				addrs = addressHashMap.get(usr.getId_usr());
			}
			long timeBegin = TimeGettor.get();
			dataGeneraorIO4Hbase.writeUsr(usr, addrs);
			long timeEnd = TimeGettor.get();
			accuSec += (timeEnd - timeBegin);
		}
		System.out.println("using time of loading users data into HBase: "
				+ Millis2HHmmss.formatLongToTimeStr(accuSec));
	}

	public void writeVisit2HBase() throws Exception {
		ArrayList<Usr> usrList = dGenerator.getUsrList();
		HashMap<String, ArrayList<Visit>> visitMap = dGenerator.getVisitMap();
		long accu = (long) 0;
		for (int i = 0; i < usrList.size(); i++) {
			Usr usr = usrList.get(i);
			ArrayList<Visit> visits = null;

			if (visitMap.containsKey(usr.getId_usr())) {
				visits = visitMap.get(usr.getId_usr());
			}

			GetIntFromId getIntFromId = new GetIntFromId(usr.getId_usr(), "usr");
			String usrId = getIntFromId.restore(TransformInteger.transform(
					getIntFromId.get(), DataGeneratorIO.NB_USR));

			ArrayList<Visit> newVisitList = null;
			if (visits != null) {
				newVisitList = new ArrayList<Visit>();
				for (int j = 0; j < visits.size(); j++) {
					Visit visit = visits.get(j);
					getIntFromId = new GetIntFromId(visit.getIdItem(), "item");
					int k = getIntFromId.get();
					String newIntegerString = TransformInteger.transform(k,
							DataGeneratorIO.NB_ITEM);
					String itemId = getIntFromId.restore(newIntegerString);

					Visit newVisit = new Visit(usrId, itemId, visit.getBuy(),
							visit.getYear(), visit.getMonth(), visit.getDay(),
							visit.getHour(), visit.getMinute(),
							visit.getSecond());
					newVisitList.add(newVisit);

				}
			}
			long timeBegin = TimeGettor.get();
			dataGeneraorIO4Hbase.writeVisit(usrId, newVisitList);
			long timeEnd = TimeGettor.get();
			accu += (timeEnd - timeBegin);
		}
		System.out.println("using time of loading visits data into HBase : "
				+ Millis2HHmmss.formatLongToTimeStr(accu));
	}

	public void writeFriendhip2HBase() throws IOException {
		ArrayList<Usr> usrList = dGenerator.getUsrList();
		HashMap<String, ArrayList<String>> friendshipMap = dGenerator
				.getFriendshipMap();
		long accu = (long) 0;
		for (int i = 0; i < usrList.size(); i++) {
			Usr usr = usrList.get(i);
			ArrayList<String> friends = null;
			if (friendshipMap.containsKey(usr.getId_usr())) {
				friends = friendshipMap.get(usr.getId_usr());
			}

			ArrayList<String> newFriendList = null;
			GetIntFromId getIntFromId = new GetIntFromId(usr.getId_usr(), "usr");
			String usrId = getIntFromId.restore(TransformInteger.transform(
					getIntFromId.get(), DataGeneratorIO.NB_USR));

			if (friends != null) {
				newFriendList = new ArrayList<String>();
				for (int j = 0; j < friends.size(); j++) {
					String friendId = friends.get(j);
					getIntFromId = new GetIntFromId(friendId, "usr");
					friendId = getIntFromId.restore(TransformInteger.transform(
							getIntFromId.get(), DataGeneratorIO.NB_USR));
					newFriendList.add(friendId);

				}
			}
			long timeBegin = TimeGettor.get();
			dataGeneraorIO4Hbase.writeFriend(usrId, newFriendList);
			long timeEnd = TimeGettor.get();
			accu += (timeEnd - timeBegin);
		}
		System.out.println("using time of loading friends data into HBase : "
				+ Millis2HHmmss.formatLongToTimeStr(accu));
	}

	public void writeItem2Postgresql() throws IOException {
		DataGeneratorIO4Postgresql.copyItem(conn2Postgresql);
	}

	public void writeItem2HBase() throws IOException {
		ArrayList<Item> items = dGenerator.getItemList();
		long accu = (long) 0;
		for (int i = 0; i < items.size(); i++) {
			Item item = items.get(i);
			long timeBegin = TimeGettor.get();
			dataGeneraorIO4Hbase.writeItem(item);
			long timeEnd = TimeGettor.get();
			accu += (timeEnd - timeBegin);
		}
		System.out.println("using time of loading items data into HBase : "
				+ Millis2HHmmss.formatLongToTimeStr(accu));
	}

	public void writeFriends2Postgresql() throws IOException {
		DataGeneratorIO4Postgresql.copyFriend(conn2Postgresql);
	}

	public void writeAddress2Postgresql() throws IOException {
		DataGeneratorIO4Postgresql.copyAddress(conn2Postgresql);
	}

	public void writeOrders2Postgresql() throws IOException {
		DataGeneratorIO4Postgresql.copyOrder(conn2Postgresql);
	}

	public void writeLineItem2Postgresql() throws IOException {
		DataGeneratorIO4Postgresql.copyLineItem(conn2Postgresql);
	}

	public void writeOrder2HBase() throws IOException {
		HashMap<String, ArrayList<LineItem>> itemOfOrderMap = dGenerator
				.getItemOfOrderMap();
		// System.out.println("nb of itemlines in HBase : " +
		// itemOfOrderMap.size());
		ArrayList<Ordered> orderList = new ArrayList<Ordered>();
		for (int i = 0; i < dGenerator.getOrderList().size(); i++) {
			Ordered ordered = dGenerator.getOrderList().get(i);
			Ordered temp = new Ordered(ordered.getIdOrder(),
					ordered.getIdUsr(), ordered.getIdShop(), ordered.getYear(),
					ordered.getMonth(), ordered.getDay(),
					ordered.getTotalPrice());
			orderList.add(temp);
		}
		long accu = (long) 0;
		for (int i = 0; i < orderList.size(); i++) {
			Ordered order = orderList.get(i);
			String orderId = order.getIdOrder();
			if (itemOfOrderMap.containsKey(orderId)) {
				ArrayList<LineItem> lineItems = itemOfOrderMap.get(orderId);
				long timeBegin = TimeGettor.get();
				dataGeneraorIO4Hbase.writeOrder(order, lineItems);
				long timeEnd = TimeGettor.get();
				accu += (timeEnd - timeBegin);
			}

		}
		System.out.println("using time of loading orders data into HBase: "
				+ Millis2HHmmss.formatLongToTimeStr(accu));
	}

	public void writeVisit2Postgresql() throws Exception {
		DataGeneratorIO4Postgresql.copyVisit(conn2Postgresql);
	}

	public void writeUsr2OracleNosql() throws IOException {
		HashMap<String, ArrayList<Address>> addrs = dGenerator.getAddressMap();
		HashMap<String, ArrayList<String>> friends = dGenerator
				.getFriendshipMap();
		HashMap<String, ArrayList<Visit>> visits = dGenerator.getVisitMap();
		ArrayList<Usr> users = dGenerator.getUsrList();
		long accu = (long) 0;
		System.out.println("nb of users : " + users.size());
		for (int i = 0; i < NB_USR; i++) {
			Usr usr = users.get(i);
			ArrayList<Address> addrList = new ArrayList<Address>();
			ArrayList<String> friendList = new ArrayList<String>();
			ArrayList<Visit> visitList = new ArrayList<Visit>();
			if (addrs.containsKey(usr.getId_usr())) {
				addrList = addrs.get(usr.getId_usr());
			}

			if (friends.containsKey(usr.getId_usr())) {
				friendList = friends.get(usr.getId_usr());
			}

			if (visits.containsKey(usr.getId_usr())) {
				visitList = visits.get(usr.getId_usr());
			}

			avro.Usr avroUsr = new avro.Usr();
			avroUsr.setName(usr.getName());
			avroUsr.setAge(usr.getAge());
			avroUsr.setSex(Character.toString(usr.getSex()));
			avroUsr.setUsrId(usr.getId_usr());
			// avroUsr.setFriendship(friendList);
			ArrayList<avro.Address> avroAddrs = new ArrayList<avro.Address>();
			ArrayList<avro.Visit> avroVisits = new ArrayList<avro.Visit>();
			for (int j = 0; j < addrList.size(); j++) {
				Address address = addrList.get(j);
				avro.Address addrAvro = new avro.Address();
				addrAvro.setCity(address.getCity());
				addrAvro.setCountry(address.getCountry());
				addrAvro.setStreet(address.getStreet());
				avroAddrs.add(addrAvro);
			}

			for (int j = 0; j < visitList.size(); j++) {
				Visit visit = visitList.get(j);
				avro.Visit visitAvro = new avro.Visit();
				visitAvro.setBuy(Character.toString(visit.getBuy()));
				visitAvro.setDate(visit.getYear() + "-" + visit.getMonth()
						+ "-" + visit.getDay());
				visitAvro.setTime(visit.getHour() + ":" + visit.getMinute()
						+ ":" + visit.getSecond());
				visitAvro.setItemId(visit.getIdItem());
				avroVisits.add(visitAvro);
			}

			for (int j = 0; j < avroAddrs.size(); j++) {
				avro.Address address = avroAddrs.get(j);
				if (countryUsrsHashMap.containsKey(address.getCountry())) {
					countryUsrsHashMap.get(address.getCountry()).add(avroUsr);
				} else {
					ArrayList<avro.Usr> usrList = new ArrayList<avro.Usr>();
					usrList.add(avroUsr);
					countryUsrsHashMap.put(address.getCountry(), usrList);
				}
			}

			for (int j = 0; j < avroVisits.size(); j++) {
				avro.Visit visit = avroVisits.get(j);
				if (itemUsrsHashMap.containsKey(visit.getItemId())) {
					UsrBuy usrBuy = new UsrBuy(avroUsr.getUsrId(),
							avroUsr.getName(), avroUsr.getSex(),
							avroUsr.getAge(), visit.getBuy());
					itemUsrsHashMap.get(visit.getItemId()).add(usrBuy);
				} else {
					ArrayList<avro.UsrBuy> usrs = new ArrayList<avro.UsrBuy>();
					UsrBuy usrBuy = new UsrBuy(avroUsr.getUsrId(),
							avroUsr.getName(), avroUsr.getSex(),
							avroUsr.getAge(), visit.getBuy());
					usrs.add(usrBuy);
					itemUsrsHashMap.put(visit.getItemId(), usrs);
				}
			}
			long timeBegin = TimeGettor.get();
			dataGeneratorIO4OracleNosql.writeUsr(usr.getId_usr(), avroUsr,
					new Visits(avroVisits), new Friendship(friendList),
					new Addresses(avroAddrs));
			long timeEnd = TimeGettor.get();
			accu += (timeEnd - timeBegin);
		}
		System.out
				.println("using time of loading users data into Oracle Nosql: "
						+ Millis2HHmmss.formatLongToTimeStr(accu));
	}

	/**
	 * run after writing usr
	 */
	public void writeCountryUsrs2OracleNosql() {
		long accu = (long) 0;
		System.out.println("nb of countryUsers : " + countryUsrsHashMap.size());
		for (Entry<String, ArrayList<avro.Usr>> entry : countryUsrsHashMap
				.entrySet()) {
			long timeBegin = TimeGettor.get();
			dataGeneratorIO4OracleNosql.writeCountryUsrs(entry.getKey(),
					new avro.Usrs(entry.getValue()));
			long timeEnd = TimeGettor.get();
			accu += (timeEnd - timeBegin);
		}
		System.out
				.println("using time of loading country-users data into Oracle Nosql: "
						+ Millis2HHmmss.formatLongToTimeStr(accu));
	}

	/**
	 * run after writing usr
	 */
	public void writeItemUsrs2OracleNosql() {
		long accu = (long) 0;
		System.out.println("nb of itemUsers : " + itemUsrsHashMap.size());
		for (Entry<String, ArrayList<avro.UsrBuy>> entry : itemUsrsHashMap
				.entrySet()) {
			long timeBegin = TimeGettor.get();
			dataGeneratorIO4OracleNosql.writeItemUsrs(entry.getKey(),
					new avro.ItemUsrs(entry.getKey(), entry.getValue()));
			long timeEnd = TimeGettor.get();
			accu += (timeEnd - timeBegin);
		}
		System.out
				.println("using time of loading item-users data into Oracle Nosql: "
						+ Millis2HHmmss.formatLongToTimeStr(accu));
	}

	public void writeShop2OracleNosql() throws IOException {
		ArrayList<Shop> shops = dGenerator.getShopList();
		HashMap<String, ArrayList<Item>> itemsHashMap = dGenerator
				.getShopItems();
		long accu = (long) 0;
		System.out.println("nb of shops : " + shops.size());
		for (int i = 0; i < shops.size(); i++) {
			Shop shop = shops.get(i);
			avro.Shop shopAvro = new avro.Shop();
			shopAvro.setShopName(shop.getName());
			shopAvro.setUrl(shop.getUrl());
			List<avro.Item> itemListAvro = new ArrayList<avro.Item>();
			ArrayList<Item> itemList = itemsHashMap.get(shop.getId_shop());
			if (itemList != null) {
				for (int j = 0; j < itemList.size(); j++) {
					Item item = itemList.get(j);
					avro.Item itemAvro = new avro.Item();
					itemAvro.setDescription(item.getDescription());
					itemAvro.setName(item.getName());
					itemAvro.setPrice((double) item.getPrice());
					itemAvro.setShopId(item.getId_shop());
					itemAvro.setUrl(item.getUrl());
					itemListAvro.add(itemAvro);
				}
			}
			avro.Items items = null;
			if (!itemListAvro.isEmpty()) {
				items = new Items(itemListAvro);
			}
			long timeBegin = TimeGettor.get();
			dataGeneratorIO4OracleNosql.writeShop(shop.getId_shop(), shopAvro,
					items);
			long timeEnd = TimeGettor.get();
			accu += (timeEnd - timeBegin);
		}
		System.out
				.println("using time of loading shops data into Oracle Nosql: "
						+ Millis2HHmmss.formatLongToTimeStr(accu));
	}

	public void writeItem2OracleNosql() {
		ArrayList<Item> items = dGenerator.getItemList();
		long accu = (long) 0;
		System.out.println("nb of items : " + items.size());
		for (int i = 0; i < items.size(); i++) {
			Item item = items.get(i);
			avro.Item itemAvro = new avro.Item();
			itemAvro.setDescription(item.getDescription());
			itemAvro.setName(item.getName());
			itemAvro.setPrice((double) item.getPrice());
			itemAvro.setUrl(item.getUrl());
			itemAvro.setShopId("");
			long timeBegin = TimeGettor.get();
			dataGeneratorIO4OracleNosql.writeItem(item.getId_item(), itemAvro);
			long timeEnd = TimeGettor.get();
			accu += (timeEnd - timeBegin);
		}
		System.out
				.println("using time of loading items data into Oracle Nosql: "
						+ Millis2HHmmss.formatLongToTimeStr(accu));
	}

	public void writeOrder2OracleNosql() {
		ArrayList<Ordered> ordereds = dGenerator.getOrderList();
		HashMap<String, ArrayList<LineItem>> itemLines = dGenerator
				.getItemOfOrderMap();
		long accu = (long) 0;

		for (int i = 0; i < ordereds.size(); i++) {
			Ordered ordered = ordereds.get(i);

			avro.Ordered orderAvro = new avro.Ordered();
			orderAvro.setDate(ordered.getYear() + "-" + ordered.getMonth()
					+ "-" + ordered.getDay());
			orderAvro.setTotalPrice((double) ordered.getTotalPrice());
			orderAvro.setUsrId(ordered.getIdUsr());

			ArrayList<LineItem> list;
			ArrayList<avro.ItemLine> itemLinesAvro = new ArrayList<avro.ItemLine>();
			String orderIdString = ordered.getIdOrder();
			if (itemLines.containsKey(orderIdString)) {
				list = itemLines.get(ordered.getIdOrder());
				for (int j = 0; j < list.size(); j++) {
					LineItem k = list.get(j);
					avro.ItemLine itemLineAvro = new ItemLine();
					itemLineAvro.setItemId(k.getItemId());
					itemLineAvro.setQuantity(k.getQuantity());
					itemLinesAvro.add(itemLineAvro);
				}
			}

			// orderAvro.setItemline(itemLinesAvro);
			for (int j = 0; j < itemLinesAvro.size(); j++) {
				ItemLine itemLine = itemLinesAvro.get(j);
				if (orderQuantityHashMap.containsKey(itemLine.getItemId())) {
					orderQuantityHashMap.get(itemLine.getItemId()).add(
							new OrderQuantity(ordered.getIdOrder(), itemLine
									.getQuantity()));
				} else {
					ArrayList<avro.OrderQuantity> oqArrayList = new ArrayList<OrderQuantity>();
					oqArrayList.add(new OrderQuantity(ordered.getIdOrder(),
							itemLine.getQuantity()));
					orderQuantityHashMap.put(itemLine.getItemId(), oqArrayList);
				}
			}

			long timeBegin = TimeGettor.get();
			if (!itemLinesAvro.isEmpty()) {
				dataGeneratorIO4OracleNosql.writeOrder(ordered.getIdOrder(),
						orderAvro, new ItemLines(itemLinesAvro));
			}
			long timeEnd = TimeGettor.get();
			accu += (timeEnd - timeBegin);
		}
		System.out
				.println("using time of loading orders data into Oracle Nosql: "
						+ Millis2HHmmss.formatLongToTimeStr(accu));
	}

	public void writeOrderQuantity2OracleNosql() {
		long accu = (long) 0;
		System.out.println("nb of orderQuantity : "
				+ orderQuantityHashMap.size());
		for (Entry<String, ArrayList<OrderQuantity>> entry : orderQuantityHashMap
				.entrySet()) {
			long timeBegin = TimeGettor.get();
			dataGeneratorIO4OracleNosql.writeOrderQuantity(entry.getKey(),
					new OrderQuantitys(entry.getValue()));
			long timeEnd = TimeGettor.get();
			accu += (timeEnd - timeBegin);
		}
		System.out
				.println("using time of loading order-quantity data into Oracle Nosql: "
						+ Millis2HHmmss.formatLongToTimeStr(accu));
	}

	public static void main(String[] args) throws Exception {
		DataGeneratorIO dataGeneratorIO = new DataGeneratorIO("my_data",
				"postgres", "fenghuang");
		// dataGeneratorIO.writeShops2Postgresql();
		System.out.println("shops data writing finish ...");

		// dataGeneratorIO.writeUsers2Postgresql();
		System.out.println("users data writing finish ...");
		// dataGeneratorIO.writeItem2Postgresql();
		System.out.println("items data writing finish ...");

		// dataGeneratorIO.writeAddress2Postgresql();
		System.out.println("addresses data writing finish ...");
		// dataGeneratorIO.writeFriends2Postgresql();
		System.out.println("friends data writing finish ...");
		// dataGeneratorIO.writeOrderData();
		System.out.println("orders data writing finish ...");
		// dataGeneratorIO.writeVisit2Postgresql();
		System.out.println("visits data writing finish ...");

		dataGeneratorIO.writeLineItem2Postgresql();

		dataGeneratorIO.writeUsr2OracleNosql();
		System.out.println("all finish ...");
	}

}
