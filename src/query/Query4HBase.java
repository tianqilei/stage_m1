package query;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.util.Bytes;

import tools.Bytes2Int;
import tools.GetIntFromId;
import tools.GetUsrIdFromString;
import tools.Millis2HHmmss;
import tools.PrintSortedMap;
import tools.TimeGettor;
import tools.TransformInteger;
import vip2p_copy.ArrayIterator;
import vip2p_copy.MemoryHashJoin;
import vip2p_copy.NRSMD;
import vip2p_copy.NTuple;
import vip2p_copy.SimplePredicate;
import vip2p_copy.SimpleProjection;
import vip2p_copy.SimpleSelection;
import vip2p_copy.TupleMetadataType;
import vip2p_copy.VIP2PExecutionException;
import conn2database.Conn2Hbase;
import dataGenerator.DataGeneratorIO;
import entityOfPostgresql.Friend;

// query 1 : tout le profil de user t.q id="usr1".
// query 2 : l'�ge de tous les utilisateurs de country : "Fr".
// query 3 : le nombre de fois que chaque items a �t� achet�.
// query 4 : les 10 items les plus vendus.
// query 5 : les 10 items pas vendu du tout.
// query 6 : les 100 pages les plus visit�es
// query 7 : les amis ayant achet� le m�me produit
public class Query4HBase {

	private final static String USR_ID_FOR_QUERY1 = "usr50";
	private final static String COUNTRY_NAME_FOR_QUERY2 = "tappets";
	private final static int NB_ITEM_PLUS_VENDU = 10;
	private final static int NB_ITEM_PAS_VENDU_DU_TOUT = 10;
	private final static int LES_PAGES_PLUS_VISITES = 100;
	private final static String ORDER_ID_4_QUERY9 = "order565";
	private final static String SHOP_ID_4_QUERY10 = "shop9992";
	private final static String SHOP_ID_4_QUERY11 = "shop9992";
	private static long timeGlobalBegin;
	private static long timeGlobalEnd;

	public static void runQuery11() throws IOException {
		Conn2Hbase conn2Hbase = new Conn2Hbase();
		HTable table = conn2Hbase.connHTable("shop");
		timeGlobalBegin = TimeGettor.get();
		long accu_datastore = (long) 0;
		int nbItem = 0;
		double totalPrice = 0;
		String shopIdString = SHOP_ID_4_QUERY11;
		GetIntFromId getIntFromId = new GetIntFromId(shopIdString, "shop");
		String itemId = getIntFromId.restore(TransformInteger.transform(
				getIntFromId.get(), DataGeneratorIO.NB_SHOP));
		String itemIdStop = getIntFromId.restore(TransformInteger.transform(
				getIntFromId.get() + 1, DataGeneratorIO.NB_SHOP));

		Scan s = new Scan(Bytes.toBytes(itemId), Bytes.toBytes(itemIdStop));
		long storeTimeBegin = TimeGettor.get();
		// get all the sales information for the current item
		ResultScanner ss = table.getScanner(s);
		long storeTimeEnd = TimeGettor.get();

		for (Result r : ss) {
			for (KeyValue kv : r.raw()) {
				String familyString = new String(kv.getFamily());
				if (familyString.equals("items")) {
					String qName = new String(kv.getQualifier());
					if (qName.equals("price")) {
						totalPrice += Double.parseDouble(new String(kv
								.getValue()));
						nbItem++;
					}
				}
			}
		}
		System.out.println(totalPrice / nbItem);
		timeGlobalEnd = TimeGettor.get();
		accu_datastore = storeTimeEnd - storeTimeBegin;

		System.out.println("the datastore query time for query 1 in HBase : "
				+ Millis2HHmmss.formatLongToTimeStr(accu_datastore));
		System.out.println("the execution time for query 1 in HBase : "
				+ Millis2HHmmss.formatLongToTimeStr(accu_datastore));
		System.out.println("the global execution time for query 1 in HBase : "
				+ Millis2HHmmss.formatLongToTimeStr(timeGlobalEnd
						- timeGlobalBegin));
	}

	public static void runQuery10() throws IOException {
		Conn2Hbase conn2Hbase = new Conn2Hbase();
		HTable table = conn2Hbase.connHTable("shop");
		timeGlobalBegin = TimeGettor.get();
		long accu_datastore = (long) 0;
		String shopIdString = SHOP_ID_4_QUERY10;
		GetIntFromId getIntFromId = new GetIntFromId(shopIdString, "shop");
		String itemId = getIntFromId.restore(TransformInteger.transform(
				getIntFromId.get(), DataGeneratorIO.NB_SHOP));
		String itemIdStop = getIntFromId.restore(TransformInteger.transform(
				getIntFromId.get() + 1, DataGeneratorIO.NB_SHOP));
		Scan s = new Scan(Bytes.toBytes(itemId), Bytes.toBytes(itemIdStop));
		long storeTimeBegin = TimeGettor.get();
		// get all the sales information for the current item
		ResultScanner ss = table.getScanner(s);
		long storeTimeEnd = TimeGettor.get();
		timeGlobalEnd = TimeGettor.get();
		accu_datastore = storeTimeEnd - storeTimeBegin;

		for (Result r : ss) {
			for (KeyValue kv : r.raw()) {
				String familyString = new String(kv.getFamily());
				if (familyString.equals("items")) {
					String vaString = new String(kv.getValue());
					System.out.println("value " + vaString);
				}
			}
		}

		System.out.println("the datastore query time for query 1 in HBase : "
				+ Millis2HHmmss.formatLongToTimeStr(accu_datastore));
		System.out.println("the execution time for query 1 in HBase : "
				+ Millis2HHmmss.formatLongToTimeStr(accu_datastore));
		System.out.println("the global execution time for query 1 in HBase : "
				+ Millis2HHmmss.formatLongToTimeStr(timeGlobalEnd
						- timeGlobalBegin));
	}

	public static void runQuery9() throws IOException {
		Conn2Hbase conn2Hbase = new Conn2Hbase();
		HTable table = conn2Hbase.connHTable("orderitem");
		timeGlobalBegin = TimeGettor.get();
		GetIntFromId getIntFromId = new GetIntFromId(ORDER_ID_4_QUERY9, "order");
		String orderId = getIntFromId.restore(TransformInteger.transform(
				getIntFromId.get(), DataGeneratorIO.NB_ORDER));
		String orderIdStop = getIntFromId.restore(TransformInteger.transform(
				getIntFromId.get() + 1, DataGeneratorIO.NB_ORDER));
		Scan s = new Scan(Bytes.toBytes(orderId), Bytes.toBytes(orderIdStop));
		System.out.println(orderId + " " + orderIdStop);
		long storeTimeBegin = TimeGettor.get();
		ResultScanner ss = table.getScanner(s);
		long storeTimeEnd = TimeGettor.get();
		int quantity = 0;
		double price = 0;
		for (Result r : ss) {
			for (KeyValue kv : r.raw()) {
				String qName = new String(kv.getQualifier());
				if (qName.equals("total_price")) {
					price = Float.parseFloat(new String(kv.getValue()));
				}
				if (qName.equals("quantity")) {
					quantity += Integer.parseInt(new String(kv.getValue()));
				}
			}
		}
		double averagePrice = price / quantity;
		System.out.println(averagePrice);
		timeGlobalEnd = TimeGettor.get();

		System.out.println("the datastore query time for query 9 in HBase : "
				+ Millis2HHmmss.formatLongToTimeStr(storeTimeEnd
						- storeTimeBegin));
		System.out.println("the execution time for query 9 in HBase : "
				+ Millis2HHmmss.formatLongToTimeStr(timeGlobalEnd
						- storeTimeBegin));
		System.out.println("the global execution time for query 9 in HBase : "
				+ Millis2HHmmss.formatLongToTimeStr(timeGlobalEnd
						- timeGlobalBegin));
	}

	public static void runQuery8() throws IOException {
		float max = 0;
		Conn2Hbase conn2Hbase = new Conn2Hbase();
		HTable table = conn2Hbase.connHTable("item");
		timeGlobalBegin = TimeGettor.get();
		Scan s = new Scan();
		s.addColumn(Bytes.toBytes("info"), Bytes.toBytes("price"));
		long storeTimeBegin = TimeGettor.get();
		ResultScanner ss = table.getScanner(s);
		long storeTimeEnd = TimeGettor.get();
		for (Result r : ss) {
			for (KeyValue kv : r.raw()) {
				float price = Float.parseFloat(new String(kv.getValue()));
				if (price > max) {
					max = price;
				}
			}
		}
		timeGlobalEnd = TimeGettor.get();

		System.out.println(max);
		System.out.println("the datastore query time for query 8 in HBase : "
				+ Millis2HHmmss.formatLongToTimeStr(storeTimeEnd
						- storeTimeBegin));
		System.out.println("the execution time for query 8 in HBase : "
				+ Millis2HHmmss.formatLongToTimeStr(timeGlobalEnd
						- storeTimeBegin));
		System.out.println("the global execution time for query 8 in HBase : "
				+ Millis2HHmmss.formatLongToTimeStr(timeGlobalEnd
						- timeGlobalBegin));
	}

	public static void runQuery1() throws IOException {
		HTable table = new HTable(Conn2Hbase.getConf(), "usr");
		timeGlobalBegin = TimeGettor.get();
		ArrayList<String> returnFamily = new ArrayList<String>();
		returnFamily.add("info");
		returnFamily.add("country");

		GetIntFromId getIntFromId = new GetIntFromId(USR_ID_FOR_QUERY1, "usr");
		String usrId = getIntFromId.restore(TransformInteger.transform(
				getIntFromId.get(), DataGeneratorIO.NB_USR));

		Get g = new Get(Bytes.toBytes(usrId));

		if (!returnFamily.isEmpty()) {
			for (int i = 0; i < returnFamily.size(); i++) {
				g.addFamily(Bytes.toBytes(returnFamily.get(i)));
			}
		}

		long timeBegin = TimeGettor.get();
		Result r = table.get(g);
		long timeEnd = TimeGettor.get();
		timeGlobalEnd = TimeGettor.get();
		table.close();

		System.out.println("the datastore query time for query 10 in HBase : "
				+ Millis2HHmmss.formatLongToTimeStr(timeEnd - timeBegin));
		System.out.println("the execution time for query 10 in HBase : "
				+ Millis2HHmmss.formatLongToTimeStr(timeEnd - timeBegin));
		System.out.println("the global execution time for query 10 in HBase : "
				+ Millis2HHmmss.formatLongToTimeStr(timeGlobalEnd
						- timeGlobalBegin));

		for (KeyValue kv : r.raw()) {
			String qualifierString;
			System.out.println("Family :" + new String(kv.getFamily()) + " "
					+ "qualifier :"
					+ (qualifierString = new String(kv.getQualifier())));
			if (qualifierString.equals("age")) {
				System.out.println("value :" + Bytes2Int.getInt(kv.getValue()));
			} else {
				System.out.println("value: " + new String(kv.getValue()));
			}
		}

	}

	public static void runQuery2() throws IOException {
		HTable table = new HTable(Conn2Hbase.getConf(), "usr");
		timeGlobalBegin = TimeGettor.get();
		List<String> arr = new ArrayList<String>();
		arr.add("country,c0," + COUNTRY_NAME_FOR_QUERY2);
		ArrayList<String> returnFamilyColArrayList = new ArrayList<String>();
		returnFamilyColArrayList.add("info,age");
		returnFamilyColArrayList.add("country,c0");
		FilterList filterList = new FilterList();
		Scan s1 = new Scan();
		for (String v : arr) {
			String[] s = v.split(",");
			filterList.addFilter(new SingleColumnValueFilter(Bytes
					.toBytes(s[0]), Bytes.toBytes(s[1]), CompareOp.EQUAL, Bytes
					.toBytes(s[2])));

			// s1.addColumn(Bytes.toBytes(s[0]), Bytes.toBytes(s[1]));
		}
		if (!returnFamilyColArrayList.isEmpty()) {
			for (String v : returnFamilyColArrayList) {
				String[] s = v.split(",");
				s1.addColumn(Bytes.toBytes(s[0]), Bytes.toBytes(s[1]));
			}
		}
		s1.setFilter(filterList);

		long timeBegin = TimeGettor.get();
		ResultScanner ResultScannerFilterList = table.getScanner(s1);
		long timeEnd = TimeGettor.get();
		table.close();
		timeGlobalEnd = TimeGettor.get();
		System.out.println("the datastore query time for query 2 in HBase : "
				+ Millis2HHmmss.formatLongToTimeStr(timeEnd - timeBegin));
		System.out.println("the execution time for query 2 in HBase : "
				+ Millis2HHmmss.formatLongToTimeStr(timeEnd - timeBegin));
		System.out.println("the global execution time for query 2 in HBase : "
				+ Millis2HHmmss.formatLongToTimeStr(timeGlobalEnd
						- timeGlobalBegin));

		for (Result rr = ResultScannerFilterList.next(); rr != null; rr = ResultScannerFilterList
				.next()) {
			for (KeyValue kv : rr.list()) {
				String rowString = new String(kv.getRow());
				String familyString = new String(kv.getFamily());
				String qualifierString = new String(kv.getQualifier());
				String valueString = new String(kv.getValue());
				if (valueString == null || valueString.equals("")) {
					continue;
				}

				System.out.println("row : " + rowString);
				System.out.println("family : " + familyString);
				System.out.println("qualifier : " + qualifierString);
				if (qualifierString.equals("age")) {
					System.out.println("value :"
							+ Bytes2Int.getInt(kv.getValue()));
				} else {
					System.out.println("value: " + new String(kv.getValue()));
				}
			}
		}

	}

	/*
	 * 1st way : because we want to get the sales of each item, I use "get" in
	 * database 10,000 times (1 item, 1 time) and I calculate the sales for each
	 * item.
	 */
	public static void runQuery3() throws IOException {
		Conn2Hbase conn2Hbase = new Conn2Hbase();
		HTable table = conn2Hbase.connHTable("ordered");
		timeGlobalBegin = TimeGettor.get();
		long accu_exe = (long) 0;
		long accu_datastore = (long) 0;
		HashMap<String, Integer> itemNbHashMap = new HashMap<String, Integer>();

		// for each item, I do one iteration to get its sales
		for (int i = 0; i < DataGeneratorIO.NB_ITEM; i++) {
			String itemIdString = "item" + i;
			String itemIdStopString = "item" + (i + 1);

			/*
			 * transformation the id of item. For example : item4 -> item004
			 */
			GetIntFromId getIntFromId = new GetIntFromId(itemIdString, "item");
			String itemId = getIntFromId.restore(TransformInteger.transform(
					getIntFromId.get(), DataGeneratorIO.NB_ITEM));
			getIntFromId = new GetIntFromId(itemIdStopString, "item");

			String itemIdStop = getIntFromId.restore(TransformInteger
					.transform(getIntFromId.get(), DataGeneratorIO.NB_ITEM));

			Scan s = new Scan(Bytes.toBytes(itemId), Bytes.toBytes(itemIdStop));

			String qualifierName = "";
			// get the beginning time
			long storeTimeBegin = TimeGettor.get();
			// get all the sales information for the current item
			ResultScanner ss = table.getScanner(s);
			long storeTimeEnd = TimeGettor.get();
			accu_datastore = storeTimeEnd - storeTimeBegin;
			int quantity = 0;
			for (Result r : ss) {
				for (KeyValue kv : r.raw()) {
					qualifierName = kv.getQualifier().toString();
					if (qualifierName.equals("quantity")) {
						// calculate its sales
						quantity += Bytes2Int.getInt(kv.getValue());
					}
				}
			}
			if (quantity > 0) {
				// put the couple (item_id, sales) into HashMap
				itemNbHashMap.put(itemId, quantity);
			}
			long timeEnd = TimeGettor.get();
			// calculate the inteval of time of this iteration
			accu_exe = (timeEnd - storeTimeBegin);
		}
		timeGlobalEnd = TimeGettor.get();
		// print the time of running query 3
		System.out
				.println("the datastore query time for query 3 by using 1st method in HBase: "
						+ Millis2HHmmss.formatLongToTimeStr(accu_datastore));
		System.out
				.println("the execution time for query 3 by using 1st method in HBase : "
						+ Millis2HHmmss.formatLongToTimeStr(accu_exe));
		System.out
				.println("the global execution time for query 3 by using 1st method in HBase : "
						+ Millis2HHmmss.formatLongToTimeStr(timeGlobalEnd
								- timeGlobalBegin));
		/*
		 * for (Entry<String, Integer> entry : itemNbHashMap.entrySet()) {
		 * System.out.println("item id : " + entry.getKey() + " nb : " +
		 * entry.getValue()); }
		 */

	}

	/*
	 * Instead of "get" 10,000 time in database, I use a "scan" only 1 time.
	 * Firstly, I create a HashMap<itemId, sales>, and During scanning the whole
	 * table, I update the HashMap, so at last, I get the sales of each item in
	 * this HashMap.
	 */
	public static void runQuery3_2() throws IOException {
		HTable table = new HTable(Conn2Hbase.getConf(), "ordered");
		timeGlobalBegin = TimeGettor.get();
		ArrayList<String> returnFamilyCols = new ArrayList<String>();
		returnFamilyCols.add("info,quantity");
		Scan s1 = new Scan();

		// return which column, in this case, we need only the column :
		// quantity(sales)
		if (!returnFamilyCols.isEmpty()) {
			for (String v : returnFamilyCols) {
				String[] s = v.split(",");
				s1.addColumn(Bytes.toBytes(s[0]), Bytes.toBytes(s[1]));
			}
		}

		HashMap<String, Integer> itemNbHashMap = new HashMap<String, Integer>();
		String rowString = "";
		String itemId = "";
		String qualifierString = "";
		String valueString = "";
		// get the beginning time
		long timeBegin = TimeGettor.get();
		// scan the whore table
		ResultScanner ResultScannerFilterList = table.getScanner(s1);
		long timeEndDatastore = TimeGettor.get();
		long timeStore = timeEndDatastore - timeBegin;
		// scan the result to calculate the sales of each item
		for (Result rr = ResultScannerFilterList.next(); rr != null; rr = ResultScannerFilterList
				.next()) {
			for (KeyValue kv : rr.list()) {
				rowString = new String(kv.getRow(), "UTF-8");
				itemId = rowString.substring(0, rowString.indexOf("/"));
				qualifierString = kv.getQualifier().toString();
				valueString = kv.getValue().toString();
				if (valueString == null || valueString.equals("")) {
					continue;
				} else if (qualifierString.equals("quantity")) {
					int quantity = Bytes2Int.getInt(kv.getValue());
					if (itemNbHashMap.containsKey(itemId)) {
						// update the sales of item with id=itemId
						itemNbHashMap.put(itemId, itemNbHashMap.get(itemId)
								+ quantity);
					} else {
						// update the sales of item with id=itemId
						itemNbHashMap.put(itemId, quantity);
					}
				}
			}
		}
		long timeEndExe = TimeGettor.get();
		System.out
				.println("the datastore query time for query 3 by using 2nd method in HBase : "
						+ Millis2HHmmss.formatLongToTimeStr(timeStore));
		System.out
				.println("the execution time for query 3 by using 2nd method in HBase : "
						+ Millis2HHmmss.formatLongToTimeStr(timeEndExe
								- timeBegin));
		System.out
				.println("the global execution time for query 3 by using 2nd method in HBase : "
						+ Millis2HHmmss.formatLongToTimeStr(timeEndExe
								- timeGlobalBegin));
		// System.out.println(itemNbHashMap.size());
		for (Entry<String, Integer> entry : itemNbHashMap.entrySet()) {
			System.out.println("key : " + entry.getKey() + " Value : "
					+ entry.getValue());
		}

		table.close();
	}

	public static void runQuery4() throws IOException {
		Conn2Hbase conn2Hbase = new Conn2Hbase();
		HTable table = conn2Hbase.connHTable("visit");
		timeGlobalBegin = TimeGettor.get();
		long accuDatastore = (long) 0;
		long accuExe = (long) 0;
		HashMap<String, Integer> itemVisitNbHashMap = new HashMap<String, Integer>();

		for (int i = 0; i < DataGeneratorIO.NB_ITEM; i++) {
			int nb = 0;
			String itemId = "item"
					+ TransformInteger.transform(i, DataGeneratorIO.NB_ITEM);
			String itemIdStop = "item"
					+ TransformInteger
							.transform(i + 1, DataGeneratorIO.NB_ITEM);

			Scan s = new Scan(Bytes.toBytes(itemId), Bytes.toBytes(itemIdStop));

			String qualifierName = "";
			String buyString = "";
			long timeBegin = TimeGettor.get();
			ResultScanner ss = table.getScanner(s);
			long timeBeginDatastore = TimeGettor.get();
			accuDatastore += (timeBeginDatastore - timeBegin);
			for (Result r : ss) {
				for (KeyValue kv : r.raw()) {
					qualifierName = kv.getQualifier().toString();
					if (qualifierName.equals("buy")) {
						buyString = kv.getValue().toString();
						if (buyString.equals("y")) {
							nb++;
						} else
							continue;
					} else
						continue;
				}
			}
			if (nb != 0) {
				itemVisitNbHashMap.put(itemId, nb);
			}
			long timeEndExe = TimeGettor.get();
			accuExe += (timeEndExe - timeBegin);
		}
		timeGlobalEnd = TimeGettor.get();
		System.out
				.println("the datastore quert time for query 4 by using 1st method in HBase : "
						+ Millis2HHmmss.formatLongToTimeStr(accuDatastore));
		System.out
				.println("the execution time for query 4 by using 1st method in HBase : "
						+ Millis2HHmmss.formatLongToTimeStr(accuExe));
		System.out
				.println("the global time for query 4 by using 1st method in HBase : "
						+ Millis2HHmmss.formatLongToTimeStr(timeGlobalEnd
								- timeGlobalBegin));

		// PrintSortedMap.printf(itemVisitNbHashMap, NB_ITEM_PLUS_VENDU);
	}

	public static void runQuery4_2() throws IOException {
		HTable table = new HTable(Conn2Hbase.getConf(), "visit");
		timeGlobalBegin = TimeGettor.get();
		FilterList filterList = new FilterList();
		List<String> arr = new ArrayList<String>();
		arr.add("info,buy,y");
		Scan s1 = new Scan();
		for (String v : arr) {
			String[] s = v.split(",");
			filterList.addFilter(new SingleColumnValueFilter(Bytes
					.toBytes(s[0]), Bytes.toBytes(s[1]), CompareOp.EQUAL, Bytes
					.toBytes(s[2])));

			// s1.addColumn(Bytes.toBytes(s[0]), Bytes.toBytes(s[1]));
		}

		s1.setFilter(filterList);
		HashMap<String, Integer> itemNbVisit = new HashMap<String, Integer>();
		String rowString = "";
		String itemId = "";

		long timeBegin = TimeGettor.get();
		ResultScanner ResultScannerFilterList = table.getScanner(s1);
		long timeEndDatastore = TimeGettor.get();
		long timeStore = timeEndDatastore - timeBegin;
		for (Result rr = ResultScannerFilterList.next(); rr != null; rr = ResultScannerFilterList
				.next()) {
			for (KeyValue kv : rr.list()) {
				rowString = new String(kv.getRow(), "UTF-8");
				itemId = rowString.substring(0, rowString.indexOf("/"));
				if (itemNbVisit.containsKey(itemId)) {
					itemNbVisit.put(itemId, itemNbVisit.get(itemId) + 1);
				} else {
					itemNbVisit.put(itemId, 1);
				}
			}
		}
		ArrayList<Entry<String, Integer>> res = PrintSortedMap.getSortedList(
				itemNbVisit, NB_ITEM_PLUS_VENDU);

		ArrayList<Map.Entry<String, Integer>> l = new ArrayList<Map.Entry<String, Integer>>(
				itemNbVisit.entrySet());
		Collections.sort(l, new Comparator<Map.Entry<String, Integer>>() {
			public int compare(Map.Entry<String, Integer> o1,
					Map.Entry<String, Integer> o2) {
				return (o2.getValue() - o1.getValue());
			}
		});

		table.close();
		long timeEndExe = TimeGettor.get();
		System.out
				.println("the datastore query time for query 4 by using 2nd method in HBase : "
						+ Millis2HHmmss.formatLongToTimeStr(timeStore));
		System.out
				.println("the execution time for query 4 by using 2nd method in HBase : "
						+ Millis2HHmmss.formatLongToTimeStr(timeEndExe
								- timeBegin));
		System.out
				.println("the global execution time for query 4 by using 2nd method in HBase : "
						+ Millis2HHmmss.formatLongToTimeStr(timeEndExe
								- timeGlobalBegin));
	}

	public static void runQuery5() throws IOException {
		Conn2Hbase conn2Hbase = new Conn2Hbase();
		HTable table = conn2Hbase.connHTable("visit");
		timeGlobalBegin = TimeGettor.get();
		long accuExe = (long) 0;
		long accuDatastore = (long) 0;
		HashMap<String, Integer> itemVisitNbHashMap = new HashMap<String, Integer>();

		for (int i = 0; i < DataGeneratorIO.NB_ITEM; i++) {
			int nb = 0;
			String itemId = "item"
					+ TransformInteger.transform(i, DataGeneratorIO.NB_ITEM);
			String itemIdStop = "item"
					+ TransformInteger
							.transform(i + 1, DataGeneratorIO.NB_ITEM);

			Scan s = new Scan(Bytes.toBytes(itemId), Bytes.toBytes(itemIdStop));

			String qualifierName = "";
			String buyString = "";
			long timeBegin = TimeGettor.get();
			ResultScanner ss = table.getScanner(s);
			long timeEnd = TimeGettor.get();
			accuDatastore += (timeEnd - timeBegin);

			for (Result r : ss) {
				for (KeyValue kv : r.raw()) {
					qualifierName = kv.getQualifier().toString();
					if (qualifierName.equals("buy")) {
						buyString = kv.getValue().toString();
						if (buyString.equals("n")) {
							nb++;
						} else
							continue;
					} else
						continue;
				}
			}
			if (nb != 0) {
				itemVisitNbHashMap.put(itemId, nb);
			}
			timeEnd = TimeGettor.get();
			accuExe += (timeEnd - timeBegin);
		}
		timeGlobalEnd = TimeGettor.get();
		System.out
				.println("the datastore query time for query 5 by using the 1st method in HBase : "
						+ Millis2HHmmss.formatLongToTimeStr(accuDatastore));
		System.out
				.println("the execution time for query 5 by using the 1st method in HBase : "
						+ Millis2HHmmss.formatLongToTimeStr(accuExe));
		System.out
				.println("the global execution time for query 5 by using the 1st method in HBase : "
						+ Millis2HHmmss.formatLongToTimeStr(timeGlobalEnd
								- timeGlobalBegin));

		// PrintSortedMap.printf(itemVisitNbHashMap, NB_ITEM_PAS_VENDU_DU_TOUT);
	}

	public static void runQuery5_2() throws IOException {
		HTable table = new HTable(Conn2Hbase.getConf(), "visit");
		timeGlobalBegin = TimeGettor.get();
		FilterList filterList = new FilterList();
		List<String> arr = new ArrayList<String>();
		arr.add("info,buy,n");
		Scan s1 = new Scan();
		for (String v : arr) {
			String[] s = v.split(",");
			filterList.addFilter(new SingleColumnValueFilter(Bytes
					.toBytes(s[0]), Bytes.toBytes(s[1]), CompareOp.EQUAL, Bytes
					.toBytes(s[2])));

			// s1.addColumn(Bytes.toBytes(s[0]), Bytes.toBytes(s[1]));
		}

		s1.setFilter(filterList);
		HashMap<String, Integer> itemNbVisit = new HashMap<String, Integer>();

		String rowString = "";
		String itemId = "";
		long timeBegin = TimeGettor.get();
		ResultScanner ResultScannerFilterList = table.getScanner(s1);
		long timeEndDatastore = TimeGettor.get();
		for (Result rr = ResultScannerFilterList.next(); rr != null; rr = ResultScannerFilterList
				.next()) {
			for (KeyValue kv : rr.list()) {
				rowString = new String(kv.getRow());
				itemId = rowString.substring(0, rowString.indexOf("/"));
				if (itemNbVisit.containsKey(itemId)) {
					itemNbVisit.put(itemId, itemNbVisit.get(itemId) + 1);
				} else {
					itemNbVisit.put(itemId, 1);
				}
			}
		}

		ArrayList<Entry<String, Integer>> res = PrintSortedMap.getSortedList(
				itemNbVisit, NB_ITEM_PAS_VENDU_DU_TOUT);

		ArrayList<Map.Entry<String, Integer>> l = new ArrayList<Map.Entry<String, Integer>>(
				itemNbVisit.entrySet());

		Collections.sort(l, new Comparator<Map.Entry<String, Integer>>() {
			public int compare(Map.Entry<String, Integer> o1,
					Map.Entry<String, Integer> o2) {
				return (o2.getValue() - o1.getValue());
			}
		});

		table.close();
		long timeEndExe = TimeGettor.get();
		System.out
				.println("the datastore query time for query 5 by using the 2nd method in HBase : "
						+ Millis2HHmmss.formatLongToTimeStr(timeEndDatastore
								- timeBegin));
		System.out
				.println("the execution time for query 5 by using the 2nd method in HBase : "
						+ Millis2HHmmss.formatLongToTimeStr(timeEndExe
								- timeBegin));
		System.out
				.println("the global execution time for query 5 by using the 2nd method in HBase : "
						+ Millis2HHmmss.formatLongToTimeStr(timeEndExe
								- timeGlobalBegin));
	}

	public static void runQuery6() throws IOException, VIP2PExecutionException {
		Conn2Hbase conn2Hbase = new Conn2Hbase();
		HTable table = conn2Hbase.connHTable("visit");
		timeGlobalBegin = TimeGettor.get();
		long accu = (long) 0;
		HashMap<String, Integer> itemVisitNbHashMap = new HashMap<String, Integer>();

		long exeTimeBegin = TimeGettor.get();
		for (int i = 0; i < DataGeneratorIO.NB_ITEM; i++) {
			int nb = 0;
			String itemId = "item"
					+ TransformInteger.transform(i, DataGeneratorIO.NB_ITEM);
			String itemIdStop = "item"
					+ TransformInteger
							.transform(i + 1, DataGeneratorIO.NB_ITEM);

			Scan s = new Scan(Bytes.toBytes(itemId), Bytes.toBytes(itemIdStop));
			long timeBegin = TimeGettor.get();
			ResultScanner ss = table.getScanner(s);
			long timeEnd = TimeGettor.get();
			accu += (timeEnd - timeBegin);
			for (Result r : ss) {
				for (KeyValue kv : r.raw()) {
					nb++;
				}

			}
			if (nb != 0) {
				itemVisitNbHashMap.put(itemId, nb);
			}
		}

		ArrayList<Entry<String, Integer>> resultList = PrintSortedMap
				.getSortedList(itemVisitNbHashMap, LES_PAGES_PLUS_VISITES);

		TupleMetadataType[] types = new TupleMetadataType[2];
		types[0] = TupleMetadataType.STRING_TYPE;
		types[1] = TupleMetadataType.INTEGER_TYPE;
		String[] colNames = new String[2];
		colNames[0] = "itemId";
		colNames[1] = "nbVisit";
		NRSMD[] nestedChildren = new NRSMD[0];
		NRSMD nrsmd = new NRSMD(2, types, colNames, nestedChildren);
		ArrayList<NTuple> vArrayList = new ArrayList<NTuple>();

		for (int i = 0; i < resultList.size(); i++) {
			char[][] stringFields = new char[1][];
			stringFields[0] = resultList.get(i).getKey().toCharArray();
			int[] integerFields = new int[1];
			integerFields[0] = resultList.get(i).getValue();
			ArrayList<NTuple>[] nestedFields = new ArrayList[0];
			vArrayList.add(new NTuple(nrsmd, stringFields, integerFields,
					nestedFields));
		}

		ArrayIterator leftArrayIterator = new ArrayIterator(vArrayList, nrsmd);

		HashMap<String, String> itemUrlHashMap = new HashMap<String, String>();
		table = conn2Hbase.connHTable("item");
		Scan s2 = new Scan();
		long timeBegin = TimeGettor.get();
		ResultScanner ss = table.getScanner(s2);
		long timeEnd = TimeGettor.get();
		accu += (timeEnd - timeBegin);
		for (Result r : ss) {
			String itemId = new String(r.getRow());
			for (KeyValue kv : r.raw()) {
				String qualifier = new String(kv.getQualifier());
				if (qualifier.equals("url")) {
					String url = new String(kv.getValue());
					itemUrlHashMap.put(itemId, url);
				}
			}
		}

		TupleMetadataType[] types2 = new TupleMetadataType[2];
		types2[0] = TupleMetadataType.STRING_TYPE;
		types2[1] = TupleMetadataType.STRING_TYPE;
		String[] colNames2 = new String[2];
		colNames2[0] = "itemId";
		colNames2[1] = "url";
		NRSMD[] nestedChildren2 = new NRSMD[0];
		NRSMD nrsmd2 = new NRSMD(2, types2, colNames2, nestedChildren2);
		ArrayList<NTuple> v2 = new ArrayList<NTuple>();
		for (Entry<String, String> entry : itemUrlHashMap.entrySet()) {

			char[][] stringFields = new char[2][];
			stringFields[0] = entry.getKey().toCharArray();
			stringFields[1] = entry.getValue().toCharArray();
			int[] integerFields = new int[0];
			ArrayList<NTuple>[] nestedFields = new ArrayList[0];
			v2.add(new NTuple(nrsmd2, stringFields, integerFields, nestedFields));
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
				.println("the datastore query time for query 5 by using the 2nd method in HBase : "
						+ Millis2HHmmss.formatLongToTimeStr(accu));
		System.out
				.println("the execution time for query 5 by using the 2nd method in HBase : "
						+ Millis2HHmmss.formatLongToTimeStr(timeGlobalEnd
								- exeTimeBegin));
		System.out
				.println("the global execution time for query 5 by using the 2nd method in HBase : "
						+ Millis2HHmmss.formatLongToTimeStr(timeGlobalEnd
								- timeGlobalBegin));
		/*
		 * projection.open(); int i = 0; while (projection.hasNext()) { NTuple
		 * nt = projection.next(); System.out.println("url : " + new
		 * String(nt.stringFields[0]) + " nb : " + nt.integerFields[0]); i++; if
		 * (i >= 100) { break; } } projection.close();
		 */

	}

	public static void runQuery6_2() throws IOException,
			VIP2PExecutionException {
		Conn2Hbase conn2Hbase = new Conn2Hbase();
		HTable tableItem = conn2Hbase.connHTable("item");
		HTable tableVisit = conn2Hbase.connHTable("visit");
		timeGlobalBegin = TimeGettor.get();
		long accuStore = (long) 0;
		HashMap<String, Integer> itemVisitNbHashMap = new HashMap<String, Integer>();

		TupleMetadataType[] types = new TupleMetadataType[2];
		types[0] = TupleMetadataType.STRING_TYPE;
		types[1] = TupleMetadataType.INTEGER_TYPE;
		String[] colNames = new String[2];
		colNames[0] = "itemId";
		colNames[1] = "nbVisit";
		NRSMD[] nestedChildren = new NRSMD[0];
		NRSMD nrsmd = new NRSMD(2, types, colNames, nestedChildren);
		ArrayList<NTuple> vArrayList = new ArrayList<NTuple>();

		TupleMetadataType[] types2 = new TupleMetadataType[2];
		types2[0] = TupleMetadataType.STRING_TYPE;
		types2[1] = TupleMetadataType.STRING_TYPE;
		String[] colNames2 = new String[2];
		colNames2[0] = "itemId";
		colNames2[1] = "url";
		NRSMD[] nestedChildren2 = new NRSMD[0];
		NRSMD nrsmd2 = new NRSMD(2, types2, colNames2, nestedChildren2);
		ArrayList<NTuple> v2 = new ArrayList<NTuple>();

		long exeTimeBegin = TimeGettor.get();
		long timeBegin = TimeGettor.get();
		Scan s = new Scan();
		ResultScanner ss = tableVisit.getScanner(s);
		long timeEnd = TimeGettor.get();
		accuStore += (timeEnd - timeBegin);
		for (Result r : ss) {
			for (KeyValue kv : r.raw()) {
				String rowString = new String(kv.getRow());
				String[] strings = rowString.split("/");
				String itemId = strings[0];
				if (itemVisitNbHashMap.containsKey(itemId)) {
					itemVisitNbHashMap.put(itemId,
							itemVisitNbHashMap.get(itemId) + 1);
				} else
					itemVisitNbHashMap.put(itemId, 1);
			}
		}

		ArrayList<Entry<String, Integer>> resultList = PrintSortedMap
				.getSortedList(itemVisitNbHashMap, LES_PAGES_PLUS_VISITES);
		for (int i = 0; i < resultList.size(); i++) {
			char[][] stringFields = new char[1][];
			stringFields[0] = resultList.get(i).getKey().toCharArray();
			int[] integerFields = new int[1];
			integerFields[0] = resultList.get(i).getValue();
			ArrayList<NTuple>[] nestedFields = new ArrayList[0];
			vArrayList.add(new NTuple(nrsmd, stringFields, integerFields,
					nestedFields));
		}

		ArrayIterator ai1 = new ArrayIterator(vArrayList, nrsmd);

		/*
		 * int p = 0; ai1.open(); while (ai1.hasNext()) { NTuple nTuple =
		 * ai1.next(); String itemId = new String(nTuple.stringFields[0]); int k
		 * = nTuple.integerFields[0]; System.out.println(itemId + " " + k); p++;
		 * } System.out.println(p);
		 */
		timeBegin = TimeGettor.get();
		Scan s2 = new Scan();
		ss = tableItem.getScanner(s2);
		timeEnd = TimeGettor.get();
		accuStore += (timeEnd - timeBegin);
		for (Result r : ss) {
			String itemId = new String(r.getRow());
			for (KeyValue kv : r.raw()) {
				String qualifier = new String(kv.getQualifier());
				if (qualifier.equals("url")) {
					String url = new String(kv.getValue());
					char[][] stringFields = new char[2][];
					stringFields[0] = itemId.toCharArray();
					stringFields[1] = url.toCharArray();
					int[] integerFields = new int[0];
					ArrayList<NTuple>[] nestedFields = new ArrayList[0];
					v2.add(new NTuple(nrsmd2, stringFields, integerFields,
							nestedFields));
				}
			}
		}
		ArrayIterator ai2 = new ArrayIterator(v2, nrsmd2);

		/*
		 * ai2.open(); while (ai2.hasNext()) { NTuple nTuple = ai2.next();
		 * String itemId = new String(nTuple.stringFields[0]); String urlString
		 * = new String(nTuple.stringFields[1]); System.out.println(itemId + " "
		 * + urlString); } ai2.close();
		 */

		MemoryHashJoin memoryHashJoin = new MemoryHashJoin(ai1, ai2,
				new SimplePredicate(0, 2));
		int[] keepColumns = new int[2];
		keepColumns[0] = 3;
		keepColumns[1] = 1;
		SimpleProjection projection = new SimpleProjection(memoryHashJoin,
				keepColumns);

		/*
		 * projection.open(); while (projection.hasNext()) { NTuple nTuple =
		 * projection.next(); String urlString = new
		 * String(nTuple.stringFields[0]); int nb = nTuple.integerFields[0];
		 * System.out.println(urlString + " " + nb); }
		 */

		timeGlobalEnd = TimeGettor.get();
		System.out
				.println("the datastore query time for query 6 by using the 2nd method in HBase : "
						+ Millis2HHmmss.formatLongToTimeStr(accuStore));
		System.out
				.println("the execution time for query 6 by using the 2nd method in HBase : "
						+ Millis2HHmmss.formatLongToTimeStr(timeGlobalEnd
								- exeTimeBegin));
		System.out
				.println("the global execution time for query 6 by using the 2nd method in HBase : "
						+ Millis2HHmmss.formatLongToTimeStr(timeGlobalEnd
								- timeGlobalBegin));

		projection.open();
		int i = 0;
		while (projection.hasNext()) {
			NTuple nt = projection.next();
			System.out.println("url : " + new String(nt.stringFields[0])
					+ " nb : " + nt.integerFields[0]);
			i++;
			if (i >= 100) {
				break;
			}
		}
		projection.close();

	}

	public static void runQuery7() throws IOException, VIP2PExecutionException {
		Conn2Hbase conn2Hbase = new Conn2Hbase();
		HTable tableFriendship = conn2Hbase.connHTable("friendship");
		HTable tableVisit = conn2Hbase.connHTable("visit");

		timeGlobalBegin = TimeGettor.get();
		long accu = (long) 0;
		ArrayList<Friend> friends = new ArrayList<Friend>();

		long exeTimeBegin = TimeGettor.get();
		for (int i = 0; i < DataGeneratorIO.NB_USR; i++) {
			String usrId = "usr"
					+ TransformInteger.transform(i, DataGeneratorIO.NB_USR);
			String usrIdStop = "usr"
					+ TransformInteger.transform(i + 1, DataGeneratorIO.NB_USR);

			Scan s = new Scan(Bytes.toBytes(usrId), Bytes.toBytes(usrIdStop));
			long timeBegin = TimeGettor.get();
			ResultScanner ss = tableFriendship.getScanner(s);
			long timeEnd = TimeGettor.get();
			accu += (timeEnd - timeBegin);
			for (Result r : ss) {
				for (KeyValue kv : r.raw()) {
					String qualifierString = new String(kv.getQualifier());
					if (qualifierString.equals("friendid")) {
						String friendid = new String(kv.getValue());
						boolean exists = false;
						for (int j = 0; j < friends.size(); j++) {
							String usr1 = friends.get(j).getIdUsr1();
							String usr2 = friends.get(j).getIdUsr2();
							if ((usr1.equals(usrId) && usr2.equals(friendid) || (usr1
									.equals(friendid) && usr2.equals(usrId)))) {
								exists = true;
							}
						}

						if (!exists) {
							Friend friend = new Friend(usrId, friendid);
							friends.add(friend);
						}
					}
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
		for (int i = 0; i < friends.size(); i++) {
			char[][] stringFields = new char[2][];
			stringFields[0] = friends.get(i).getIdUsr1().toCharArray();
			stringFields[1] = friends.get(i).getIdUsr2().toCharArray();
			int[] integerFields = new int[0];
			ArrayList<NTuple>[] nestedFields = new ArrayList[0];
			v.add(new NTuple(nrsmd, stringFields, integerFields, nestedFields));
		}

		ArrayIterator leftArrayIterator = new ArrayIterator(v, nrsmd);

		ArrayList<UsrBuyItem> usrItemList = new ArrayList<UsrBuyItem>();
		for (int i = 0; i < DataGeneratorIO.NB_ITEM; i++) {
			String itemId = "item"
					+ TransformInteger.transform(i, DataGeneratorIO.NB_ITEM);
			String itemIdStop = "item"
					+ TransformInteger
							.transform(i + 1, DataGeneratorIO.NB_ITEM);

			Scan s = new Scan(Bytes.toBytes(itemId), Bytes.toBytes(itemIdStop));
			long timeBegin = TimeGettor.get();
			ResultScanner ss = tableVisit.getScanner(s);
			long timeEnd = TimeGettor.get();
			accu += (timeEnd - timeBegin);
			for (Result r : ss) {
				for (KeyValue kv : r.raw()) {
					String usrId = GetUsrIdFromString.getUsrId(new String(kv
							.getRow()));
					if (new String(kv.getQualifier()).equals("buy")) {
						if (new String(kv.getValue()).equals("y")) {
							UsrBuyItem usrBuyItem = new UsrBuyItem(usrId,
									itemId);
							usrItemList.add(usrBuyItem);
						}
					}
				}
			}
		}

		TupleMetadataType[] types2 = new TupleMetadataType[2];
		types2[0] = TupleMetadataType.STRING_TYPE;
		types2[1] = TupleMetadataType.STRING_TYPE;
		String[] colNames2 = new String[2];
		colNames2[0] = "usrId";
		colNames2[1] = "itemId";
		NRSMD[] nestedChildren2 = new NRSMD[0];
		NRSMD nrsmd2 = new NRSMD(2, types2, colNames2, nestedChildren2);
		ArrayList<NTuple> v2 = new ArrayList<NTuple>();
		for (int i = 0; i < usrItemList.size(); i++) {

			char[][] stringFields = new char[2][];
			stringFields[0] = usrItemList.get(i).getUsrId().toCharArray();
			stringFields[1] = usrItemList.get(i).getItemId().toCharArray();
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
				.println("the datastore query time for query 5 by using the 2nd method in HBase : "
						+ Millis2HHmmss.formatLongToTimeStr(accu));
		System.out
				.println("the execution time for query 5 by using the 2nd method in HBase : "
						+ Millis2HHmmss.formatLongToTimeStr(timeGlobalEnd
								- exeTimeBegin));
		System.out
				.println("the global execution time for query 5 by using the 2nd method in HBase : "
						+ Millis2HHmmss.formatLongToTimeStr(timeGlobalEnd
								- timeGlobalBegin));
		/*
		 * projection.open(); while (projection.hasNext()) { NTuple nt =
		 * projection.next(); System.out.println("usr id1 : " + new
		 * String(nt.stringFields[0]) + "; usr id2 : " + new
		 * String(nt.stringFields[1]) + "; item id : " + new
		 * String(nt.stringFields[2])); } projection.close();
		 */
	}

	public static void runQuery7_2() throws IOException,
			VIP2PExecutionException {
		Conn2Hbase conn2Hbase = new Conn2Hbase();
		HTable tableFriends = conn2Hbase.connHTable("friendship");
		HTable tableVisits = conn2Hbase.connHTable("visit");
		timeGlobalBegin = TimeGettor.get();
		long accuStore = (long) 0;

		long timeBegin = TimeGettor.get();
		Scan s = new Scan();
		ResultScanner ss = tableFriends.getScanner(s);
		long timeEnd = TimeGettor.get();
		accuStore += (timeEnd - timeBegin);

		TupleMetadataType[] types = new TupleMetadataType[2];
		types[0] = TupleMetadataType.STRING_TYPE;
		types[1] = TupleMetadataType.STRING_TYPE;
		String[] colNames = new String[2];
		colNames[0] = "usrId1";
		colNames[1] = "usrId2";
		NRSMD[] nestedChildren = new NRSMD[0];
		NRSMD nrsmd = new NRSMD(2, types, colNames, nestedChildren);

		TupleMetadataType[] types2 = new TupleMetadataType[2];
		types2[0] = TupleMetadataType.STRING_TYPE;
		types2[1] = TupleMetadataType.STRING_TYPE;
		String[] colNames2 = new String[2];
		colNames2[0] = "usrId";
		colNames2[1] = "itemId";
		NRSMD[] nestedChildren2 = new NRSMD[0];
		NRSMD nrsmd2 = new NRSMD(2, types2, colNames2, nestedChildren2);

		ArrayList<NTuple> nTuples = new ArrayList<NTuple>();
		ArrayList<NTuple> nTuples2 = new ArrayList<NTuple>();
		/*
		 * long exeTimeBegin = TimeGettor.get(); for (Result r : ss) { for
		 * (KeyValue kv : r.raw()) { String rowString = new String(kv.getRow());
		 * String usrId1 = rowString .subSequence(0,
		 * rowString.indexOf('/')).toString(); String usrId2 =
		 * rowString.subSequence( rowString.indexOf('/') + 1,
		 * rowString.length()) .toString(); char[][] stringFields = new
		 * char[2][]; stringFields[0] = usrId1.toCharArray(); stringFields[1] =
		 * usrId2.toCharArray(); int[] integerFields = new int[0];
		 * ArrayList<NTuple>[] nestedFields = new ArrayList[0]; NTuple nTuple =
		 * new NTuple(nrsmd, stringFields, integerFields, nestedFields);
		 * nTuples.add(nTuple); } }
		 */

		long exeTimeBegin = TimeGettor.get();
		for (Result r : ss) {
			for (KeyValue kv : r.raw()) {
				String rowString = new String(kv.getRow());
				String usrId1 = rowString
						.subSequence(0, rowString.indexOf('/')).toString();
				String usrId2 = rowString.subSequence(
						rowString.indexOf('/') + 1, rowString.length())
						.toString();

				char[][] stringFields = new char[2][];
				stringFields[0] = usrId1.toCharArray();
				stringFields[1] = usrId2.toCharArray();
				int[] integerFields = new int[0];
				ArrayList<NTuple>[] nestedFields = new ArrayList[0];
				NTuple nTuple = new NTuple(nrsmd, stringFields, integerFields,
						nestedFields);
				nTuples.add(nTuple);
			}
		}

		// friends
		ArrayIterator ai1 = new ArrayIterator(nTuples, nrsmd);

		/*
		 * ai1.open(); System.out.println(ai1.hasNext()); while (ai1.hasNext())
		 * { NTuple nTuple = ai1.next(); String usrId1 = new
		 * String(nTuple.stringFields[0]); String usrId2 = new
		 * String(nTuple.stringFields[1]); System.out.println(usrId1 + " " +
		 * usrId2); } ai1.close();
		 */

		timeBegin = TimeGettor.get();
		s = new Scan();
		ss = tableVisits.getScanner(s);
		timeEnd = TimeGettor.get();
		accuStore += (timeEnd - timeBegin);
		for (Result r : ss) {
			for (KeyValue kv : r.raw()) {
				String rowString = new String(kv.getRow());
				if (new String(kv.getQualifier()).equals("buy")) {
					if (new String(kv.getValue()).equals("y")) {
						String[] rs = rowString.split("/");
						String itemId = rs[0];
						String usrId = rs[1];
						char[][] stringFields = new char[2][];
						stringFields[0] = itemId.toCharArray();
						stringFields[1] = usrId.toCharArray();
						int[] integerFields = new int[0];
						ArrayList<NTuple>[] nestedFields = new ArrayList[0];
						NTuple nTuple = new NTuple(nrsmd2, stringFields,
								integerFields, nestedFields);
						nTuples2.add(nTuple);
					}
				}
			}
		}

		// itemId - UsrId
		ArrayIterator ai2 = new ArrayIterator(nTuples2, nrsmd2);
		/*
		 * ai2.open(); while (ai2.hasNext()) { NTuple nTuple = ai2.next();
		 * String itemId = new String(nTuple.stringFields[0]); String usrId =
		 * new String(nTuple.stringFields[1]); System.out.println(itemId + " " +
		 * usrId); } ai2.close();
		 */

		MemoryHashJoin memoryHashJoin = new MemoryHashJoin(ai1, ai2,
				new SimplePredicate(0, 3));
		memoryHashJoin = new MemoryHashJoin(ai2, memoryHashJoin,
				new SimplePredicate(1, 3));
		SimpleSelection selection = new SimpleSelection(memoryHashJoin,
				new SimplePredicate(0, 4));

		selection.open();

		int k = 0;
		while (selection.hasNext()) {
			NTuple nTuple = selection.next();
			String itemId = new String(nTuple.stringFields[0]);
			String usrID1 = new String(nTuple.stringFields[2]);
			String usrId2 = new String(nTuple.stringFields[3]);
			k++;
			System.out.println(usrID1 + " " + usrId2 + " " + itemId);
		}

		System.out.println(k);

		timeGlobalEnd = TimeGettor.get();
		System.out
				.println("the datastore query time for query 5 by using the 2nd method in HBase : "
						+ Millis2HHmmss.formatLongToTimeStr(accuStore));
		System.out
				.println("the execution time for query 5 by using the 2nd method in HBase : "
						+ Millis2HHmmss.formatLongToTimeStr(timeGlobalEnd
								- exeTimeBegin));
		System.out
				.println("the global execution time for query 5 by using the 2nd method in HBase : "
						+ Millis2HHmmss.formatLongToTimeStr(timeGlobalEnd
								- timeGlobalBegin));

	}

	public static void main(String[] args) throws IOException,
			VIP2PExecutionException {
		System.out.println("*** query 1 ***");
		// Query4HBase.runQuery1();
		System.out.println("*** query 2 ***");
		// Query4HBase.runQuery2();
		System.out.println("*** query 3 ***");
		// Query4HBase.runQuery3();
		// Query4HBase.runQuery3_2();
		System.out.println("*** query 4 ***");
		// Query4HBase.runQuery4();
		// Query4HBase.runQuery4_2();
		System.out.println("*** query 5 ***");
		// Query4HBase.runQuery5();
		// Query4HBase.runQuery5_2();
		System.out.println("*** query 6 ***");
		// Query4HBase.runQuery6();
		// Query4HBase.runQuery6_2();
		System.out.println("*** query 7 ***");
		// Query4HBase.runQuery7_2();
		// Query4HBase.runQuery7();
		System.out.println("*** query 8 ***");
		// Query4HBase.runQuery8();
		System.out.println("*** query 9 ***");
		// Query4HBase.runQuery9();
		System.out.println("*** query 10 ***");
		// Query4HBase.runQuery10();
		System.out.println("*** query 11 ***");
		Query4HBase.runQuery11();
	}
}
