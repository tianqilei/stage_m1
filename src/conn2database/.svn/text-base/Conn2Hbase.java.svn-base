package conn2database;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FamilyFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.WritableByteArrayComparable;
import org.apache.hadoop.hbase.util.Bytes;

import tools.Bytes2Int;
import tools.IPGetter;
import tools.Millis2HHmmss;
import tools.TimeGettor;

// Class that has nothing but a main.
// Does a Put, Get and a Scan against an hbase table.
public class Conn2Hbase {

	private Configuration configuration;
	private final static String HOST_IP = IPGetter.get();

	public Conn2Hbase() {
		configuration = HBaseConfiguration.create();
	}

	public HTable connHTable(String tableName) throws IOException {
		return new HTable(configuration, tableName);
	}

	public Configuration getConfiguration() {
		return configuration;
	}

	public HTable creTable(String tableName) throws IOException {
		return new HTable(configuration, tableName);
	}

	private static Configuration conf = null;
	static {
		conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", HOST_IP);
	}

	public static Configuration getConf() {
		return conf;
	}

	public void createTable(String tableName, String cols[]) throws Exception {
		HBaseAdmin admin = new HBaseAdmin(conf);
		if (admin.tableExists(tableName)) {
			admin.disableTable(tableName);
			admin.deleteTable(tableName);
		}
		HTableDescriptor table = new HTableDescriptor(tableName);
		for (String c : cols) {
			HColumnDescriptor col = new HColumnDescriptor(c);
			table.addFamily(col);
		}

		admin.createTable(table);
		admin.close();

		System.out.println("create table !");
	}

	public void insertRow(String tableName, String row, String columnFamily,
			String column, String value) throws Exception {
		HTable table = new HTable(conf, tableName);
		Put put = new Put(Bytes.toBytes(row));
		put.add(Bytes.toBytes(columnFamily), Bytes.toBytes(column),
				Bytes.toBytes(value));

		table.put(put);
		table.close();
		System.out.println("data inserted");
	}

	public void deleteByRow(String tableName, String rowkey) throws Exception {
		HTable h = new HTable(conf, tableName);
		Delete d = new Delete(Bytes.toBytes(rowkey));
		h.delete(d);
		h.close();
	}

	public void deleteByRow(String tableName, String rowkey[]) throws Exception {
		HTable h = new HTable(conf, tableName);

		List<Delete> list = new ArrayList<Delete>();
		for (String k : rowkey) {
			Delete d = new Delete(Bytes.toBytes(k));
			list.add(d);
		}
		h.delete(list);
		h.close();
	}

	public void getAllRecord(String tableName) {
		try {
			HTable table = new HTable(conf, tableName);
			Scan s = new Scan();
			ResultScanner ss = table.getScanner(s);
			for (Result r : ss) {
				for (KeyValue kv : r.raw()) {
					System.out.print("Row :" + new String(kv.getRow()) + " ");
					System.out.print(" Family :" + new String(kv.getFamily())
							+ ":");
					System.out.print(" Qualifier :"
							+ new String(kv.getQualifier()) + " ");
					System.out.println(" Value :" + new String(kv.getValue()));
				}
			}
			table.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	public static void getAllRecordByUsingParialKey(String tableName,
			String startKey, String stopKey) {
		try {
			HTable table = new HTable(conf, tableName);
			Scan s = new Scan(Bytes.toBytes(startKey), Bytes.toBytes(stopKey));

			ResultScanner ss = table.getScanner(s);
			for (Result r : ss) {
				for (KeyValue kv : r.raw()) {
					System.out.print("Row :" + new String(kv.getRow()) + " ");
					System.out.print(" Family :" + new String(kv.getFamily())
							+ ":");
					System.out.print(" Qualifier :"
							+ new String(kv.getQualifier()) + " ");
					System.out.println(" Value :" + new String(kv.getValue()));
				}
			}
			table.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	public static ResultScanner getTableScanner(String tableName)
			throws IOException {
		HTable table = new HTable(conf, tableName);
		Scan s = new Scan();
		table.close();
		return table.getScanner(s);
	}

	/**
	 * select by rowKey
	 * */
	public static Result selectByRowKey(String tablename, String rowKey,
			ArrayList<String> returnFamily) throws IOException {
		HTable table = new HTable(conf, tablename);
		Get g = new Get(Bytes.toBytes(rowKey));

		if (!returnFamily.isEmpty()) {
			for (int i = 0; i < returnFamily.size(); i++) {
				g.addFamily(Bytes.toBytes(returnFamily.get(i)));
			}
		}

		Result r = table.get(g);
		table.close();
		return r;
	}

	public static void selectByRowKeyColumn(String tablename, String rowKey,
			String family, String qualifier) throws IOException {
		HTable table = new HTable(conf, tablename);
		Get g = new Get(Bytes.toBytes(rowKey));
		g.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier));
		Result r = table.get(g);
		for (KeyValue kv : r.raw()) {
			System.out.println("column: "
					+ new String(kv.getFamily() + ":" + kv.getQualifier()));
			System.out.println("value: " + new String(kv.getValue()));
		}
		table.close();
	}

	public static void selectByFilter(String tablename, List<String> arr,
			ArrayList<String> returnFamilyCols) throws IOException {
		HTable table = new HTable(conf, tablename);
		FilterList filterList = new FilterList();
		Scan s1 = new Scan();
		for (String v : arr) {
			String[] s = v.split(",");
			filterList.addFilter(new SingleColumnValueFilter(Bytes
					.toBytes(s[0]), Bytes.toBytes(s[1]), CompareOp.EQUAL, Bytes
					.toBytes(s[2])));

			// s1.addColumn(Bytes.toBytes(s[0]), Bytes.toBytes(s[1]));
		}
		if (!returnFamilyCols.isEmpty()) {
			for (String v : returnFamilyCols) {
				String[] s = v.split(",");
				s1.addColumn(Bytes.toBytes(s[0]), Bytes.toBytes(s[1]));
			}
		}
		s1.setFilter(filterList);

		long timeBegin = TimeGettor.get();
		ResultScanner ResultScannerFilterList = table.getScanner(s1);
		long timeEnd = TimeGettor.get();
		
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
		table.close();
	}

	public void selectByFamilyFilter(String tablename, List<String> arr,
			String returnFalimy) throws IOException {
		HTable table = new HTable(conf, tablename);
		FilterList filterList = new FilterList();
		Scan s1 = new Scan();
		for (String v : arr) {
			String[] s = v.split(",");
			WritableByteArrayComparable familyComparator = new WritableByteArrayComparable() {

				@Override
				public int compareTo(byte[] arg0, int arg1, int arg2) {
					// TODO Auto-generated method stub
					String targetValueString = new String(arg0);
					if (targetValueString.equals("send")) {
						return 0;
					}
					return 1;
				}
			};

			filterList.addFilter(new FamilyFilter(CompareOp.EQUAL,
					familyComparator));

			// add this to return the named column.
			// s1.addColumn(Bytes.toBytes(s[0]), Bytes.toBytes(s[1]));
		}
		s1.setFilter(filterList);
		ResultScanner ResultScannerFilterList = table.getScanner(s1);
		for (Result rr = ResultScannerFilterList.next(); rr != null; rr = ResultScannerFilterList
				.next()) {
			for (KeyValue kv : rr.list()) {
				System.out.println("row : " + new String(kv.getRow()));
				System.out.println("Family :" + new String(kv.getFamily())
						+ " " + "qualifier :" + new String(kv.getQualifier()));
				System.out.println("value : " + new String(kv.getValue()));
			}
		}
		table.close();
	}

	public static void main(String[] args) throws IOException {
		// You need a configuration object to tell the client where to connect.
		// When you create a HBaseConfiguration, it reads in whatever you've set
		// into your hbase-site.xml and in hbase-default.xml, as long as these
		// can
		// be found on the CLASSPATH
		Configuration config = HBaseConfiguration.create();

		// This instantiates an HTable object that connects you to
		// the "myTable" table.
		HTable table = new HTable(config, "myTable");

		// To add to a row, use Put. A Put constructor takes the name of the row
		// you want to insert into as a byte array. In HBase, the Bytes class
		// has
		// utility for converting all kinds of java types to byte arrays. In the
		// below, we are converting the String "myLittleRow" into a byte array
		// to
		// use as a row key for our update. Once you have a Put instance, you
		// can
		// adorn it by setting the names of columns you want to update on the
		// row,
		// the timestamp to use in your update, etc.If no timestamp, the server
		// applies current time to the edits.
		Put p = new Put(Bytes.toBytes("myLittleRow"));

		// To set the value you'd like to update in the row 'myLittleRow',
		// specify
		// the column family, column qualifier, and value of the table cell
		// you'd
		// like to update. The column family must already exist in your table
		// schema. The qualifier can be anything. All must be specified as byte
		// arrays as hbase is all about byte arrays. Lets pretend the table
		// 'myLittleHBaseTable' was created with a family 'myLittleFamily'.
		p.add(Bytes.toBytes("myColumnFamily"), Bytes.toBytes("someQualifier"),
				Bytes.toBytes("Some Value")); // which means
												// myColumnFamily:someQualifier
												// value = Some Value

		// Once you've adorned your Put instance with all the updates you want
		// to
		// make, to commit it do the following (The HTable#put method takes the
		// Put instance you've been building and pushes the changes you made
		// into
		// hbase)
		table.put(p);

		// Now, to retrieve the data we just wrote. The values that come back
		// are
		// Result instances. Generally, a Result is an object that will package
		// up
		// the hbase return into the form you find most palatable.
		Get g = new Get(Bytes.toBytes("myLittleRow"));
		Result r = table.get(g);
		byte[] value = r.getValue(Bytes.toBytes("myColumnFamily"),
				Bytes.toBytes("someQualifier"));
		// If we convert the value bytes, we should get back 'Some Value', the
		// value we inserted at this location.
		String valueStr = Bytes.toString(value);
		System.out.println("GET: " + valueStr);

		// Sometimes, you won't know the row you're looking for. In this case,
		// you
		// use a Scanner. This will give you cursor-like interface to the
		// contents
		// of the table. To set up a Scanner, do like you did above making a Put
		// and a Get, create a Scan. Adorn it with column names, etc.
		Scan s = new Scan();
		s.addColumn(Bytes.toBytes("myColumnFamily"),
				Bytes.toBytes("someQualifier"));
		ResultScanner scanner = table.getScanner(s);
		try {
			// Scanners return Result instances.
			// Now, for the actual iteration. One way is to use a while loop
			// like so:
			for (Result rr = scanner.next(); rr != null; rr = scanner.next()) {
				// print out the row we found and the columns we were looking
				// for
				System.out.println("Found row: " + rr);
			}

			// The other approach is to use a foreach loop. Scanners are
			// iterable!
			// for (Result rr : scanner) {
			// System.out.println("Found row: " + rr);
			// }
		} finally {
			// Make sure you close your scanners when you are done!
			// Thats why we have it inside a try/finally clause
			scanner.close();
		}
		table.close();
	}

}