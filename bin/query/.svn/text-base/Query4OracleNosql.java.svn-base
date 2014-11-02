package query;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;

import org.apache.avro.specific.SpecificRecord;

import avro.Item;
import avro.ItemLine;
import avro.ItemUsrs;
import avro.OrderQuantity;
import avro.OrderQuantitys;
import avro.UsrBuy;
import avro.Usrs;
import avro.Visit;
import oracle.kv.Key;
import oracle.kv.Value;
import oracle.kv.ValueVersion;
import tools.Millis2HHmmss;
import tools.PrintSortedMap;
import tools.TimeGettor;
import vip2p_copy.ArrayIterator;
import vip2p_copy.MemoryHashJoin;
import vip2p_copy.NRSMD;
import vip2p_copy.NTuple;
import vip2p_copy.SimplePredicate;
import vip2p_copy.SimpleProjection;
import vip2p_copy.SimpleSelection;
import vip2p_copy.TupleMetadataType;
import vip2p_copy.VIP2PExecutionException;
import conn2database.Conn2OracleNosql;
import dataGenerator.DataGeneratorIO;
import entityOfPostgresql.Friend;
// query 1 : tout le profil de user t.q id="usr1".
// query 2 : l'âge de tous les utilisateurs de country : "Fr".
// query 3 : le nombre de fois que chaque items a été acheté.
// query 4 : les 10 items les plus vendus.
// query 5 : les 10 items pas vendu du tout.
// query 6 : les 100 pages les plus visitées
// query 7 : les amis ayant acheté le même produit

public class Query4OracleNosql {

	private Conn2OracleNosql conn2OracleNosql;
	private final static String USR_ID = "usr50";
	private final static String USR_ADDR = "tappets";
	private final static int NB_ITEM_MOST_SOLD = 10;
	private final static int NB_ITEM_NOT_SOLD_AT_ALL = 10;
	private final static int NB_PAGES_PLUS_VISITS = 100;
	private final static String ORDER_ID_4_QUERY9 = "order565";
	private final static String SHOP_ID_4_QUERY10 = "shop9992";
	private final static String SHOP_ID_QUERY11 = "shop9992";
	private long timeGlobalBegin;
	private long timeGlobalEnd;

	public Query4OracleNosql() throws IOException {
		conn2OracleNosql = new Conn2OracleNosql();
	}

	public void runQuery8() throws VIP2PExecutionException {
		long accuStore = (long) 0;
		timeGlobalBegin = TimeGettor.get();
		double max = 0;
		List<String> majorPath = Arrays.asList("Item");
		long timeExeBegin = TimeGettor.get();
		for (int i = 0; i < DataGeneratorIO.NB_ITEM; i++) {
			long storeBegin = TimeGettor.get();
			Double price = conn2OracleNosql.getDouble(Key.createKey(majorPath,
					Arrays.asList("item" + i, "price")));
			long storeEnd = TimeGettor.get();
			accuStore += (storeEnd - storeBegin);
			if (price > max) {
				max = price;
			}
		}
		timeGlobalEnd = TimeGettor.get();
		System.out
				.println("the stagastore query time for query 8 in Oracle Nosql : "
						+ Millis2HHmmss.formatLongToTimeStr(accuStore));
		System.out.println("the execution time for query 8 in Oracle Nosql : "
				+ Millis2HHmmss.formatLongToTimeStr(timeGlobalEnd
						- timeExeBegin));
		System.out
				.println("the global execution time for query 8 in Oracle Nosql : "
						+ Millis2HHmmss.formatLongToTimeStr(timeGlobalEnd
								- timeGlobalBegin));

		// System.out.println(max);
	}

	public void runQuery9() {
		long accuStore = (long) 0;
		timeGlobalBegin = TimeGettor.get();
		HashMap<String, Double> orderIdAveragePrice = new HashMap<String, Double>();
		List<String> majorPath = Arrays.asList("Order");
		long timeExeBegin = TimeGettor.get();
		long storeBegin = TimeGettor.get();
		SpecificRecord specificRecord = conn2OracleNosql.getRecord(Key
				.createKey(majorPath,
						Arrays.asList(ORDER_ID_4_QUERY9, "itemline")));
		Double totalPrice = conn2OracleNosql.getDouble(Key.createKey(majorPath,
				Arrays.asList(ORDER_ID_4_QUERY9, "totalprice")));
		long storeEnd = TimeGettor.get();
		accuStore += (storeEnd - storeBegin);
		avro.ItemLines itemLines = (avro.ItemLines) specificRecord;

		List<ItemLine> ils = itemLines.getItemline();

		int totalNb = 0;
		for (int j = 0; j < ils.size(); j++) {
			ItemLine itemLine = ils.get(j);
			totalNb += itemLine.getQuantity();
		}
		System.out.println(totalPrice / totalNb);
		timeGlobalEnd = TimeGettor.get();
		System.out
				.println("the store query time for query 9 in Oracle Nosql : "
						+ Millis2HHmmss.formatLongToTimeStr(accuStore));
		System.out.println("the execution time for query 9 in Oracle Nosql : "
				+ Millis2HHmmss.formatLongToTimeStr(timeGlobalEnd
						- timeExeBegin));
		System.out
				.println("the global execution time for query 9 in Oracle Nosql : "
						+ Millis2HHmmss.formatLongToTimeStr(timeGlobalEnd
								- timeGlobalBegin));
		/*
		 * for (Entry<String, Double> entry : orderIdAveragePrice.entrySet()) {
		 * System.out.println("Key : " + entry.getKey() + " Value : " +
		 * entry.getValue()); }
		 */
	}

	public void runQuery10() {
		long accu = (long) 0;
		timeGlobalBegin = TimeGettor.get();
		List<String> majorPath = Arrays.asList("Shop");
		List<String> minorPath = Arrays.asList(SHOP_ID_4_QUERY10, "items");
		Key key = Key.createKey(majorPath, minorPath);
		long timeBegin = TimeGettor.get();
		SpecificRecord specificRecord = conn2OracleNosql.getRecord(key);
		long timeEnd = TimeGettor.get();
		accu += (timeEnd - timeBegin);
		avro.Items items = (avro.Items) specificRecord;
		List<Item> itemList = items.getItemlist();
		for (int i = 0; i < itemList.size(); i++) {
			Item item = itemList.get(i);
			String nameString = item.getName();
			double price = item.getPrice();
		}
		timeGlobalEnd = TimeGettor.get();
		System.out
				.println("the datastore query time for query 2 in Oracle Nosql : "
						+ Millis2HHmmss.formatLongToTimeStr(accu));
		System.out.println("the execution time for query 2 in Oracle Nosql : "
				+ Millis2HHmmss.formatLongToTimeStr(timeGlobalEnd - timeBegin));
		System.out
				.println("the global execution time for query 2 in Oracle Nosql : "
						+ Millis2HHmmss.formatLongToTimeStr(timeGlobalEnd
								- timeGlobalBegin));
		/*
		 * List<Item> itemList = items.getItemlist(); for (int i = 0; i <
		 * itemList.size(); i++) { Item item = itemList.get(i);
		 * System.out.println(item.getName() + " " + item.getPrice()); }
		 */
	}

	public void runQuery11() {
		long accu = (long) 0;
		timeGlobalBegin = TimeGettor.get();
		List<String> majorPath = Arrays.asList("Shop");
		List<String> minorPath = Arrays.asList(SHOP_ID_4_QUERY10, "items");
		Key key = Key.createKey(majorPath, minorPath);
		long timeBegin = TimeGettor.get();
		SpecificRecord specificRecord = conn2OracleNosql.getRecord(key);
		long timeEnd = TimeGettor.get();
		accu += (timeEnd - timeBegin);
		avro.Items items = (avro.Items) specificRecord;
		List<Item> itemList = items.getItemlist();
		int nbItem = itemList.size();
		double totalPrice = 0;
		for (int i = 0; i < nbItem; i++) {
			avro.Item item = itemList.get(i);
			totalPrice += item.getPrice();
		}
		timeGlobalEnd = TimeGettor.get();
		System.out
				.println("the datastore query time for query 2 in Oracle Nosql : "
						+ Millis2HHmmss.formatLongToTimeStr(accu));
		System.out.println("the execution time for query 2 in Oracle Nosql : "
				+ Millis2HHmmss.formatLongToTimeStr(timeGlobalEnd - timeBegin));
		System.out
				.println("the global execution time for query 2 in Oracle Nosql : "
						+ Millis2HHmmss.formatLongToTimeStr(timeGlobalEnd
								- timeGlobalBegin));
		System.out.println(totalPrice / nbItem);
	}

	public void runQuery1() {
		timeGlobalBegin = TimeGettor.get();
		List<String> majorPath = Arrays.asList("Usr");
		List<String> minorPath = Arrays.asList(USR_ID, "name");
		Key key = Key.createKey(majorPath, minorPath);

		long accu = (long) 0;
		long exeTimeBegin = TimeGettor.get();
		long timeBegin = TimeGettor.get();
		ValueVersion vv = conn2OracleNosql.getValueVersion(key);
		long timeEnd = TimeGettor.get();
		accu += (timeEnd - timeBegin);

		Value value = vv.getValue();
		byte[] bytes = value.getValue();
		String name = new String(bytes);

		minorPath = Arrays.asList(USR_ID, "age");
		key = Key.createKey(majorPath, minorPath);

		timeBegin = TimeGettor.get();
		vv = conn2OracleNosql.getValueVersion(key);
		timeEnd = TimeGettor.get();
		accu += (timeEnd - timeBegin);

		value = vv.getValue();
		bytes = value.getValue();
		int age = conn2OracleNosql.bytes2Int(bytes);

		minorPath = Arrays.asList(USR_ID, "sex");
		key = Key.createKey(majorPath, minorPath);

		timeBegin = TimeGettor.get();
		vv = conn2OracleNosql.getValueVersion(key);
		timeEnd = TimeGettor.get();
		accu += (timeEnd - timeBegin);

		value = vv.getValue();
		bytes = value.getValue();
		String sex = new String(bytes);
		long exeTimeEnd = TimeGettor.get();
		timeGlobalEnd = TimeGettor.get();
		System.out
				.println("the stagastore query time for query 1 in Oracle Nosql : "
						+ Millis2HHmmss.formatLongToTimeStr(accu));
		System.out.println("the execution time for query 1 in Oracle Nosql : "
				+ Millis2HHmmss.formatLongToTimeStr(exeTimeEnd - exeTimeBegin));
		System.out
				.println("the global execution time for query 1 in Oracle Nosql : "
						+ Millis2HHmmss.formatLongToTimeStr(timeGlobalEnd
								- timeGlobalBegin));

		System.out.println("user name : " + name);
		System.out.println("user age : " + age);
		System.out.println("user sex : " + sex);

	}

	public void runQuery2() {
		timeGlobalBegin = TimeGettor.get();
		List<String> majorPath = Arrays.asList("Country", USR_ADDR);
		Key key = Key.createKey(majorPath);
		long timeBegin = TimeGettor.get();
		SpecificRecord specificRecord = conn2OracleNosql.getRecord(key);
		long timeEnd = TimeGettor.get();

		if (specificRecord == null) {
			timeGlobalEnd = TimeGettor.get();
			System.out
					.println("the datastore query time for query 2 in Oracle Nosql : "
							+ Millis2HHmmss.formatLongToTimeStr(timeEnd
									- timeBegin));
			System.out
					.println("the execution time for query 2 in Oracle Nosql : "
							+ Millis2HHmmss.formatLongToTimeStr(timeGlobalEnd
									- timeBegin));
			System.out
					.println("the global execution time for query 2 in Oracle Nosql : "
							+ Millis2HHmmss.formatLongToTimeStr(timeGlobalEnd
									- timeGlobalBegin));
			return;
		}

		Usrs usrs = (Usrs) specificRecord;
		timeGlobalEnd = TimeGettor.get();
		System.out
				.println("the datastore query time for query 2 in Oracle Nosql : "
						+ Millis2HHmmss
								.formatLongToTimeStr(timeEnd - timeBegin));
		System.out.println("the execution time for query 2 in Oracle Nosql : "
				+ Millis2HHmmss.formatLongToTimeStr(timeGlobalEnd - timeBegin));
		System.out
				.println("the global execution time for query 2 in Oracle Nosql : "
						+ Millis2HHmmss.formatLongToTimeStr(timeGlobalEnd
								- timeGlobalBegin));

		for (int i = 0; i < usrs.getUserlist().size(); i++) {
			avro.Usr usr = usrs.getUserlist().get(i);
			System.out.println(usr.getAge());
		}

	}

	public void runQuery3() {
		long accu_datastore = (long) 0;
		timeGlobalBegin = TimeGettor.get();
		HashMap<String, Integer> itemNBHashMap = new HashMap<String, Integer>();
		long exeTimeBegin = TimeGettor.get();
		for (int i = 0; i < DataGeneratorIO.NB_ITEM; i++) {
			Key key = Key.createKey(Arrays.asList("Item"),
					Arrays.asList("item" + i, "orderquantity"));
			long timeBegin = TimeGettor.get();
			SpecificRecord specificRecord = conn2OracleNosql.getRecord(key);
			long timeEnd = TimeGettor.get();
			accu_datastore += (timeEnd - timeBegin);
			OrderQuantitys orderQuantitys = (OrderQuantitys) specificRecord;
			if (orderQuantitys == null) {
				continue;
			}

			int quantity = 0;
			List<OrderQuantity> orderQuantityList = orderQuantitys.getSale();
			for (int j = 0; j < orderQuantityList.size(); j++) {
				quantity += orderQuantityList.get(j).getQuantity();
			}

			if (!itemNBHashMap.containsKey("item" + i)) {
				itemNBHashMap.put("item" + i, quantity);
			} else {
				System.err
						.println("Query4OracleNosql.java : duplicate key : item"
								+ i);
				System.exit(1);
			}
		}

		timeGlobalEnd = TimeGettor.get();
		System.out
				.println("the datastore query time for query 3 in Oracle Nosql : "
						+ Millis2HHmmss.formatLongToTimeStr(accu_datastore));
		System.out.println("the execution time for query 3 in Oracle Nosql : "
				+ Millis2HHmmss.formatLongToTimeStr(timeGlobalEnd
						- exeTimeBegin));
		System.out
				.println("the global execution time for query 3 in Oracle Nosql : "
						+ Millis2HHmmss.formatLongToTimeStr(timeGlobalEnd
								- timeGlobalBegin));

		for (Entry<String, Integer> entry : itemNBHashMap.entrySet()) {
			System.out.println("key : " + entry.getKey() + " value : "
					+ entry.getValue());
		}

	}

	public void runQuery4() {
		timeGlobalBegin = TimeGettor.get();
		List<String> majorPath = Arrays.asList("Sale");
		HashMap<String, Integer> itemSoldNbHashMap = new HashMap<String, Integer>();
		long accu_datastore = (long) 0;
		long accu_exe = (long) 0;
		for (int i = 0; i < DataGeneratorIO.NB_ITEM; i++) {
			Key key = Key.createKey(majorPath, Arrays.asList("item" + i));
			long timeBegin = TimeGettor.get();
			SpecificRecord spec = conn2OracleNosql.getRecord(key);
			long timeEnd = TimeGettor.get();
			accu_datastore += (timeEnd - timeBegin);
			ItemUsrs itemUsrs = (ItemUsrs) spec;
			int nb = 0;
			if (itemUsrs != null) {
				for (int j = 0; j < itemUsrs.getUserlist().size(); j++) {
					UsrBuy usrBuy = itemUsrs.getUserlist().get(j);
					if (usrBuy.getBuy().equals("y")) {
						nb++;
					}
				}
				itemSoldNbHashMap.put(itemUsrs.getItemid(), nb);
			}
			timeEnd = TimeGettor.get();
			accu_exe += (timeEnd - timeBegin);
		}
		ArrayList<Entry<String, Integer>> res = PrintSortedMap.getSortedList(
				itemSoldNbHashMap, NB_ITEM_MOST_SOLD);
		timeGlobalEnd = TimeGettor.get();
		System.out
				.println("the datastore query time for query 4 in Oracle Nosql : "
						+ Millis2HHmmss.formatLongToTimeStr(accu_datastore));
		System.out.println("the execution time for query 4 in Oracle Nosql : "
				+ Millis2HHmmss.formatLongToTimeStr(accu_exe));
		System.out
				.println("the global execution time for query 4 in Oracle Nosql : "
						+ Millis2HHmmss.formatLongToTimeStr(timeGlobalEnd
								- timeGlobalBegin));
		PrintSortedMap.printf(itemSoldNbHashMap, NB_ITEM_MOST_SOLD);
	}

	public void runQuery5() {
		long accu_datastore = (long) 0;
		long accu_exe = (long) 0;
		timeGlobalBegin = TimeGettor.get();
		List<String> majorPath = Arrays.asList("Sale");
		HashMap<String, Integer> itemSoldNbHashMap = new HashMap<String, Integer>();
		for (int i = 0; i < DataGeneratorIO.NB_ITEM; i++) {
			Key key = Key.createKey(majorPath, Arrays.asList("item" + i));
			long timeBegin = TimeGettor.get();
			SpecificRecord spec = conn2OracleNosql.getRecord(key);
			long timeEnd = TimeGettor.get();
			accu_datastore += (timeEnd - timeBegin);

			ItemUsrs itemUsrs = (ItemUsrs) spec;
			int nb = 0;

			if (itemUsrs != null) {
				for (int j = 0; j < itemUsrs.getUserlist().size(); j++) {
					UsrBuy usrBuy = itemUsrs.getUserlist().get(j);
					if (usrBuy.getBuy().equals("n")) {
						nb++;
					}
				}
				itemSoldNbHashMap.put(itemUsrs.getItemid(), nb);
			}
			timeEnd = TimeGettor.get();
			accu_exe += (timeEnd - timeBegin);
		}
		ArrayList<Entry<String, Integer>> res = PrintSortedMap.getSortedList(
				itemSoldNbHashMap, NB_ITEM_NOT_SOLD_AT_ALL);
		timeGlobalEnd = TimeGettor.get();
		System.out
				.println("the datastore query time for query 5 in Oracle Nosql : "
						+ Millis2HHmmss.formatLongToTimeStr(accu_datastore));
		System.out.println("the execution time for query 5 in Oracle Nosql : "
				+ Millis2HHmmss.formatLongToTimeStr(accu_exe));
		System.out
				.println("the global execution time for query 5 in Oracle Nosql : "
						+ Millis2HHmmss.formatLongToTimeStr(timeGlobalEnd
								- timeGlobalBegin));
		PrintSortedMap.printf(itemSoldNbHashMap, NB_ITEM_NOT_SOLD_AT_ALL);
	}

	public void runQuery6() throws VIP2PExecutionException {
		long accu = (long) 0;
		timeGlobalBegin = TimeGettor.get();
		List<String> majorPath = Arrays.asList("Usr");
		HashMap<String, Integer> itemIdNBHashMap = new HashMap<String, Integer>();
		long exeTimeBegin = TimeGettor.get();
		for (int i = 0; i < DataGeneratorIO.NB_USR; i++) {
			long timeBegin = TimeGettor.get();
			SpecificRecord specificRecord = conn2OracleNosql.getRecord(Key
					.createKey(majorPath, Arrays.asList("usr" + i, "visits")));
			long timeEnd = TimeGettor.get();
			accu += (timeEnd - timeBegin);
			avro.Visits visits = (avro.Visits) specificRecord;
			List<Visit> visitList = visits.getVisit();
			for (int j = 0; j < visitList.size(); j++) {
				Visit visit = visitList.get(j);
				if (itemIdNBHashMap.containsKey(visit.getItemId())) {
					itemIdNBHashMap.put(visit.getItemId(),
							itemIdNBHashMap.get(visit.getItemId()) + 1);
				} else {
					itemIdNBHashMap.put(visit.getItemId(), 1);
				}
			}
		}

		ArrayList<Entry<String, Integer>> resultList = PrintSortedMap
				.getSortedList(itemIdNBHashMap, NB_PAGES_PLUS_VISITS);

		TupleMetadataType[] types = new TupleMetadataType[2];
		types[0] = TupleMetadataType.STRING_TYPE;
		types[1] = TupleMetadataType.INTEGER_TYPE;
		String[] colNames = new String[2];
		colNames[0] = "itemId";
		colNames[1] = "nbVisit";
		NRSMD[] nestedChildren = new NRSMD[0];
		NRSMD nrsmd = new NRSMD(2, types, colNames, nestedChildren);
		ArrayList<NTuple> v = new ArrayList<NTuple>();
		for (int i = 0; i < resultList.size(); i++) {
			char[][] stringFields = new char[1][];
			stringFields[0] = resultList.get(i).getKey().toCharArray();
			int[] integerFields = new int[1];
			integerFields[0] = resultList.get(i).getValue();
			ArrayList<NTuple>[] nestedFields = new ArrayList[0];
			v.add(new NTuple(nrsmd, stringFields, integerFields, nestedFields));
		}

		ArrayIterator leftArrayIterator = new ArrayIterator(v, nrsmd);

		TupleMetadataType[] types2 = new TupleMetadataType[2];
		types2[0] = TupleMetadataType.STRING_TYPE;
		types2[1] = TupleMetadataType.STRING_TYPE;
		String[] colNames2 = new String[2];
		colNames2[0] = "itemId";
		colNames2[1] = "url";
		NRSMD[] nestedChildren2 = new NRSMD[0];
		NRSMD nrsmd2 = new NRSMD(2, types2, colNames2, nestedChildren2);
		ArrayList<NTuple> v2 = new ArrayList<NTuple>();
		majorPath = Arrays.asList("Item");
		for (int i = 0; i < DataGeneratorIO.NB_ITEM; i++) {
			long timeBegin = TimeGettor.get();
			String urlString = conn2OracleNosql.getString(Key.createKey(
					majorPath, Arrays.asList("item" + i, "url")));
			System.out.println(urlString);
			long timeEnd = TimeGettor.get();
			accu += (timeEnd - timeBegin);
			if (urlString != null) {
				char[][] stringFields = new char[2][];
				stringFields[0] = ("item" + i).toCharArray();
				stringFields[1] = urlString.toCharArray();
				int[] integerFields = new int[0];
				ArrayList<NTuple>[] nestedFields = new ArrayList[0];
				v2.add(new NTuple(nrsmd2, stringFields, integerFields,
						nestedFields));
			}
		}

		ArrayIterator rightArrayIterator = new ArrayIterator(v2, nrsmd2);
		MemoryHashJoin memoryHashJoin = new MemoryHashJoin(leftArrayIterator,
				rightArrayIterator, new SimplePredicate(0, 2));
		int[] keepColumns = new int[2];
		keepColumns[0] = 3;
		keepColumns[1] = 1;
		SimpleProjection projection = new SimpleProjection(memoryHashJoin,
				keepColumns);
		timeGlobalEnd = TimeGettor.get();
		System.out
				.println("the datastore query time for query 4 in Oracle Nosql : "
						+ Millis2HHmmss.formatLongToTimeStr(accu));
		System.out.println("the execution time for query 4 in Oracle Nosql : "
				+ Millis2HHmmss.formatLongToTimeStr(timeGlobalEnd
						- exeTimeBegin));
		System.out
				.println("the global execution time for query 4 in Oracle Nosql : "
						+ Millis2HHmmss.formatLongToTimeStr(timeGlobalEnd
								- timeGlobalBegin));

		projection.open();
		int i = 0;
		System.out.println(projection.hasNext());
		while (projection.hasNext()) {
			projection.next();
			i++;
		}
		System.out.println(i);

	}

	public void runQuery7() throws VIP2PExecutionException {
		long accu = (long) 0;
		timeGlobalBegin = TimeGettor.get();
		ArrayList<Friend> friends = new ArrayList<Friend>();
		ArrayList<UsrBuyItem> usrItemList = new ArrayList<UsrBuyItem>();
		long timeBegin = TimeGettor.get();
		for (int i = 0; i < DataGeneratorIO.NB_USR; i++) {
			String usrIdString = "usr" + i;
			Key key = Key.createKey(Arrays.asList("Usr"),
					Arrays.asList(usrIdString, "friendship"));
			long timeDBBegin = TimeGettor.get();
			SpecificRecord specificRecord = conn2OracleNosql.getRecord(key);
			long timeDBEnd = TimeGettor.get();
			accu += (timeDBEnd - timeDBBegin);
			avro.Friendship friendship = (avro.Friendship) specificRecord;
			List<String> friendsList = friendship.getFriendship();
			key = Key.createKey(Arrays.asList("Usr"),
					Arrays.asList(usrIdString, "visits"));
			timeDBBegin = TimeGettor.get();
			specificRecord = conn2OracleNosql.getRecord(key);
			timeDBEnd = TimeGettor.get();
			accu += (timeDBEnd - timeDBBegin);
			avro.Visits visits = (avro.Visits) specificRecord;
			List<Visit> visitList = visits.getVisit();

			for (int j = 0; j < friendsList.size(); j++) {
				String friendId = friendsList.get(j);
				boolean exists = false;
				for (int k = 0; k < friends.size(); k++) {
					String usr1 = friends.get(k).getIdUsr1();
					String usr2 = friends.get(k).getIdUsr2();
					if ((usr1.equals(usrIdString) && usr2.equals(friendId) || (usr1
							.equals(friendId) && usr2.equals(usrIdString)))) {
						exists = true;
					}
				}

				if (!exists) {
					Friend friend = new Friend(usrIdString, friendId);
					friends.add(friend);
				}
			}

			for (int j = 0; j < visitList.size(); j++) {
				Visit visit = visitList.get(j);
				if (visit.getBuy().equals("y")) {
					UsrBuyItem usrBuyItem = new UsrBuyItem(usrIdString,
							visit.getItemId());
					usrItemList.add(usrBuyItem);
				}
			}
		}

		TupleMetadataType[] types = new TupleMetadataType[2];
		types[0] = TupleMetadataType.STRING_TYPE;
		types[1] = TupleMetadataType.STRING_TYPE;
		String[] colNames = new String[2];
		colNames[0] = "usrId1";
		colNames[1] = "usrId2";
		NRSMD[] nestedChildren = new NRSMD[0];
		NRSMD nrsmd = new NRSMD(2, types, colNames, nestedChildren);
		ArrayList<NTuple> v = new ArrayList<NTuple>();
		for (int j = 0; j < friends.size(); j++) {
			char[][] stringFields = new char[2][];
			stringFields[0] = friends.get(j).getIdUsr1().toCharArray();
			stringFields[1] = friends.get(j).getIdUsr2().toCharArray();
			int[] integerFields = new int[0];
			ArrayList<NTuple>[] nestedFields = new ArrayList[0];
			v.add(new NTuple(nrsmd, stringFields, integerFields, nestedFields));
		}

		ArrayIterator leftArrayIterator = new ArrayIterator(v, nrsmd);

		TupleMetadataType[] types2 = new TupleMetadataType[2];
		types2[0] = TupleMetadataType.STRING_TYPE;
		types2[1] = TupleMetadataType.STRING_TYPE;
		String[] colNames2 = new String[2];
		colNames2[0] = "usrId";
		colNames2[1] = "itemId";
		NRSMD[] nestedChildren2 = new NRSMD[0];
		NRSMD nrsmd2 = new NRSMD(2, types2, colNames2, nestedChildren2);
		ArrayList<NTuple> v2 = new ArrayList<NTuple>();
		for (int j = 0; j < usrItemList.size(); j++) {

			char[][] stringFields = new char[2][];
			stringFields[0] = usrItemList.get(j).getUsrId().toCharArray();
			stringFields[1] = usrItemList.get(j).getItemId().toCharArray();
			int[] integerFields = new int[0];
			ArrayList<NTuple>[] nestedFields = new ArrayList[0];
			v2.add(new NTuple(nrsmd2, stringFields, integerFields, nestedFields));
		}
		ArrayIterator rightArrayIterator = new ArrayIterator(v2, nrsmd2);

		MemoryHashJoin memoryHashJoin = new MemoryHashJoin(leftArrayIterator,
				rightArrayIterator, new SimplePredicate(1, 2));

		memoryHashJoin = new MemoryHashJoin(rightArrayIterator, memoryHashJoin,
				new SimplePredicate(0, 2));

		SimpleSelection selection = new SimpleSelection(memoryHashJoin,
				new SimplePredicate(1, 5));
		int[] keepColumns = new int[3];
		keepColumns[0] = 0;
		keepColumns[1] = 3;
		keepColumns[2] = 5;
		SimpleProjection projection = new SimpleProjection(selection,
				keepColumns);
		timeGlobalEnd = TimeGettor.get();
		System.out
				.println("the stagastore query time for query 7 by using unnested solution in Oracle Nosql : "
						+ Millis2HHmmss.formatLongToTimeStr(accu));
		System.out
				.println("the execution time for query 7 bu using unnested solution in Oracle Nosql : "
						+ Millis2HHmmss.formatLongToTimeStr(timeGlobalEnd
								- timeBegin));
		System.out
				.println("the global execution time for query 7 by using unnested solution in Oracle Nosql : "
						+ Millis2HHmmss.formatLongToTimeStr(timeGlobalEnd
								- timeGlobalBegin));

	}

	public void runQuery7_2() throws VIP2PExecutionException {
		long accu = (long) 0;
		ArrayList<String> res = new ArrayList<String>();
		timeGlobalBegin = TimeGettor.get();
		ArrayList<NTuple> nTuples = new ArrayList<NTuple>();
		TupleMetadataType[] types = new TupleMetadataType[1];
		types[0] = TupleMetadataType.STRING_TYPE;
		String[] colNames = new String[1];
		colNames[0] = "usrId";

		NRSMD[] nestedChildren = new NRSMD[1];
		TupleMetadataType[] cTypes = new TupleMetadataType[1];
		cTypes[0] = TupleMetadataType.STRING_TYPE;
		String[] cColNames = new String[1];
		cColNames[0] = "usrList";
		NRSMD[] cNestedChildren = new NRSMD[0];

		NRSMD cNrsmd = new NRSMD(1, cTypes, cColNames, cNestedChildren);
		nestedChildren[0] = cNrsmd;
		NRSMD nrsmd = new NRSMD(1, types, colNames, nestedChildren);

		ArrayList<NTuple> nTuples2 = new ArrayList<NTuple>();
		TupleMetadataType[] cTypes2 = new TupleMetadataType[1];
		cTypes2[0] = TupleMetadataType.STRING_TYPE;
		String[] cColNames2 = new String[1];
		cColNames2[0] = "usrId";
		NRSMD[] nestedChildren2 = new NRSMD[1];

		NRSMD[] cNestedChildren2 = new NRSMD[0];
		NRSMD cNrsmd2 = new NRSMD(1, cTypes2, cColNames2, cNestedChildren2);
		nestedChildren2[0] = cNrsmd2;
		TupleMetadataType[] types2 = new TupleMetadataType[1];
		types2[0] = TupleMetadataType.STRING_TYPE;
		String[] colNames2 = new String[1];
		colNames2[0] = "itemId";
		NRSMD nrsmd2 = new NRSMD(1, types2, colNames2, nestedChildren2);

		TupleMetadataType[] types3 = new TupleMetadataType[2];
		types3[0] = TupleMetadataType.STRING_TYPE;
		types3[1] = TupleMetadataType.STRING_TYPE;
		String[] colNames3 = new String[2];
		colNames3[0] = "usrId";
		colNames3[1] = "itemId";

		NRSMD[] nestedChildren3 = new NRSMD[0];
		NRSMD nrsmd3 = new NRSMD(2, types3, colNames3, nestedChildren3);
		ArrayList<NTuple> nTuples3 = new ArrayList<NTuple>();

		long exeTimeBegin = TimeGettor.get();
		for (int i = 0; i < DataGeneratorIO.NB_USR; i++) {
			String usrIdString = "usr" + i;
			Key key = Key.createKey(Arrays.asList("Usr"),
					Arrays.asList(usrIdString, "friendship"));
			long timeBegin = TimeGettor.get();
			SpecificRecord specificRecord = conn2OracleNosql.getRecord(key);
			long timeEnd = TimeGettor.get();
			accu += (timeEnd - timeBegin);
			avro.Friendship friendship = (avro.Friendship) specificRecord;
			List<String> friendsList = friendship.getFriendship();

			char[][] stringFields = new char[1][];
			stringFields[0] = usrIdString.toCharArray();
			int[] integerFields = new int[0];
			// problem de initialisation
			ArrayList<NTuple>[] nestedFields = new ArrayList[1];
			nestedFields[0] = new ArrayList<NTuple>();
			for (int j = 0; j < friendsList.size(); j++) {
				char[][] nestedStringFields = new char[1][];
				nestedStringFields[0] = friendsList.get(j).toCharArray();
				int[] nestedIntegerFields = new int[0];
				ArrayList<NTuple>[] nestedNestedFields = new ArrayList[0];
				String[] nestedColNames = new String[1];
				nestedColNames[0] = "usr_id";
				TupleMetadataType[] nestedTypes = new TupleMetadataType[1];
				nestedTypes[0] = TupleMetadataType.STRING_TYPE;
				NRSMD[] nestedNestedChildren = new NRSMD[0];
				NRSMD nestedNrsmd = new NRSMD(1, nestedTypes, nestedColNames,
						nestedNestedChildren);
				NTuple nTuple = new NTuple(nestedNrsmd, nestedStringFields,
						nestedIntegerFields, nestedNestedFields);
				nestedFields[0].add(nTuple);
			}
			NTuple nTuple = new NTuple(nrsmd, stringFields, integerFields,
					nestedFields);
			nTuples.add(nTuple);
		}

		ArrayIterator ai1 = new ArrayIterator(nTuples, nrsmd);

		for (int i = 0; i < DataGeneratorIO.NB_ITEM; i++) {
			String itemIdString = "item" + i;
			Key key = Key.createKey(Arrays.asList("Sale"),
					Arrays.asList(itemIdString));
			long timeBegin = TimeGettor.get();
			SpecificRecord specificRecord = conn2OracleNosql.getRecord(key);
			long timeEnd = TimeGettor.get();
			accu += (timeEnd - timeBegin);
			avro.ItemUsrs itemUsrs = (avro.ItemUsrs) specificRecord;

			char[][] stringFields = new char[1][];
			stringFields[0] = itemIdString.toCharArray();
			int[] integerFields = new int[0];
			ArrayList<NTuple>[] nestedFields = new ArrayList[1];
			nestedFields[0] = new ArrayList<NTuple>();
			List<UsrBuy> usrBuys = new ArrayList<UsrBuy>();
			if (!(itemUsrs == null)) {
				usrBuys = itemUsrs.getUserlist();
			}
			for (int j = 0; j < usrBuys.size(); j++) {
				UsrBuy usrBuy = usrBuys.get(j);
				if (usrBuy.getBuy().equals("y")) {
					TupleMetadataType[] nestedTypes = new TupleMetadataType[1];
					nestedTypes[0] = TupleMetadataType.STRING_TYPE;
					String[] nestedColNames = new String[1];
					nestedColNames[0] = "usrId";
					NRSMD[] nestedNestedChildren = new NRSMD[0];
					NRSMD nestedNrsmd = new NRSMD(1, nestedTypes,
							nestedColNames, nestedNestedChildren);

					char[][] nestedStringFields = new char[1][];
					nestedStringFields[0] = usrBuy.getUsrId().toCharArray();
					int[] nestedIntegerFields = new int[0];
					ArrayList<NTuple>[] nestedNestedFields = new ArrayList[0];
					NTuple nTuple = new NTuple(nestedNrsmd, nestedStringFields,
							nestedIntegerFields, nestedNestedFields);
					nestedFields[0].add(nTuple);
				}
			}

			NTuple nTuple = new NTuple(nrsmd2, stringFields, integerFields,
					nestedFields);
			nTuples2.add(nTuple);
		}

		ArrayIterator ai2 = new ArrayIterator(nTuples2, nrsmd2);

		for (int i = 0; i < DataGeneratorIO.NB_USR; i++) {
			String usrIdString = "usr" + i;
			Key key = Key.createKey(Arrays.asList("Usr"),
					Arrays.asList(usrIdString, "visits"));
			long timeBegin = TimeGettor.get();
			SpecificRecord specificRecord = conn2OracleNosql.getRecord(key);
			long timeEnd = TimeGettor.get();
			accu += (timeEnd - timeBegin);
			avro.Visits visits = (avro.Visits) specificRecord;
			List<Visit> visitList = visits.getVisit();
			for (int j = 0; j < visitList.size(); j++) {
				if (visitList.get(j).getBuy().equals("y")) {
					String itemId = visitList.get(j).getItemId();
					char[][] stringFields = new char[2][0];
					int[] integerFields = new int[0];
					stringFields[0] = usrIdString.toCharArray();
					stringFields[1] = itemId.toCharArray();
					ArrayList<NTuple>[] nestedFields = new ArrayList[0];
					NTuple nTuple = new NTuple(nrsmd3, stringFields,
							integerFields, nestedFields);
					nTuples3.add(nTuple);
				} else {
					continue;
				}
			}
		}

		ArrayIterator ai3 = new ArrayIterator(nTuples3, nrsmd3);

		MemoryHashJoin memoryHashJoin = new MemoryHashJoin(ai1, ai3,
				new SimplePredicate(0, 1));

		memoryHashJoin = new MemoryHashJoin(memoryHashJoin, ai2,
				new SimplePredicate(2, 3));

		memoryHashJoin.open();
		while (memoryHashJoin.hasNext()) {
			ArrayList<String> sList = new ArrayList<String>();
			ArrayList<String> sList2 = new ArrayList<String>();
			NTuple nTuple = memoryHashJoin.next();
			ArrayList<NTuple> nList = nTuple.nestedFields[0];
			ArrayList<NTuple> nList2 = nTuple.nestedFields[1];
			for (int i = 0; i < nList.size(); i++) {
				NTuple m = nList.get(i);
				sList.add(new String(m.stringFields[0]));
			}
			for (int i = 0; i < nList2.size(); i++) {
				NTuple n = nList2.get(i);
				String usrIdTmp = new String(n.stringFields[0]);
				if (sList.contains(usrIdTmp)) {
					sList2.add(usrIdTmp);
				}
			}
			if (!sList2.isEmpty()) {
				String usrId = new String(nTuple.stringFields[0]);
				String itemId = new String(nTuple.stringFields[2]);
				res.add(usrId + " " + itemId + sList2);
			}
		}
		timeGlobalEnd = TimeGettor.get();
		System.out
				.println("the datastore query time for query 7 by using nested solution in Oracle Nosql : "
						+ Millis2HHmmss.formatLongToTimeStr(accu));
		System.out
				.println("the execution time for query 7 bu using nested solution in Oracle Nosql : "
						+ Millis2HHmmss.formatLongToTimeStr(timeGlobalEnd
								- exeTimeBegin));
		System.out
				.println("the global execution time for query 7 by using nested solution in Oracle Nosql : "
						+ Millis2HHmmss.formatLongToTimeStr(timeGlobalEnd
								- timeGlobalBegin));
	}

	public static void main(String[] args) throws IOException,
			VIP2PExecutionException {
		Query4OracleNosql query4OracleNosql = new Query4OracleNosql();
		System.out.println("*** Query 1 ***");
		// query4OracleNosql.runQuery1();
		System.out.println("*** Query 2 ***");
		// query4OracleNosql.runQuery2();
		System.out.println("*** Query 3 ***");
		// query4OracleNosql.runQuery3();
		System.out.println("*** Query 4 ***");
		// query4OracleNosql.runQuery4();
		System.out.println("*** Query 5 ***");
		// query4OracleNosql.runQuery5();
		System.out.println("*** Query 6 ***");
		// query4OracleNosql.runQuery6();
		System.out.println("*** Query 7 ***");
		// query4OracleNosql.runQuery7();
		// query4OracleNosql.runQuery7_2();
		System.out.println("*** Query 8 ***");
		// query4OracleNosql.runQuery8();
		System.out.println("*** Query 9 ***");
		// query4OracleNosql.runQuery9();
		System.out.println("*** Query 10 ***");
		// query4OracleNosql.runQuery10();
		System.out.println("*** Query 11 ***");
		query4OracleNosql.runQuery11();
	}

}
