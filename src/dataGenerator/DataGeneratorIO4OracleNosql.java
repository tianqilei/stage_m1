package dataGenerator;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import oracle.kv.KVStore;
import oracle.kv.Key;
import oracle.kv.Value;
import oracle.kv.avro.SpecificAvroBinding;

import org.apache.avro.specific.SpecificRecord;
import org.apache.hadoop.hbase.util.Bytes;

import avro.ItemUsrs;
import avro.OrderQuantitys;
import conn2database.Conn2OracleNosql;

public class DataGeneratorIO4OracleNosql {

	private Conn2OracleNosql conn2OracleNosql;
	private SpecificAvroBinding<SpecificRecord> binding;
	private KVStore store;

	public DataGeneratorIO4OracleNosql() throws IOException {
		conn2OracleNosql = new Conn2OracleNosql();
		binding = conn2OracleNosql.getBinding();
		store = conn2OracleNosql.getStore();

	}

	public void writeUsr(String key, avro.Usr usr, avro.Visits visits,
			avro.Friendship friends, avro.Addresses addresses)
			throws IOException {
		List<String> majorPath = Arrays.asList("Usr");
		List<String> minorPath = Arrays.asList(key, "name");
		Value value = Value.createValue(usr.getName().getBytes());
		store.put(Key.createKey(majorPath, minorPath), value);

		minorPath = Arrays.asList(key, "age");
		value = Value.createValue(Bytes.toBytes(usr.getAge()));
		store.put(Key.createKey(majorPath, minorPath), value);

		minorPath = Arrays.asList(key, "usrId");
		value = Value.createValue(Bytes.toBytes(usr.getUsrId()));
		store.put(Key.createKey(majorPath, minorPath), value);

		minorPath = Arrays.asList(key, "sex");
		value = Value.createValue(usr.getSex().getBytes());
		store.put(Key.createKey(majorPath, minorPath), value);

		minorPath = Arrays.asList(key, "visits");
		store.put(Key.createKey(majorPath, minorPath), binding.toValue(visits));

		minorPath = Arrays.asList(key, "addresses");
		store.put(Key.createKey(majorPath, minorPath),
				binding.toValue(addresses));

		minorPath = Arrays.asList(key, "friendship");
		store.put(Key.createKey(majorPath, minorPath), binding.toValue(friends));

	}

	public void writeCountryUsrs(String countryName, avro.Usrs usrs) {
		List<String> majorPath = Arrays.asList("Country", countryName);
		store.put(Key.createKey(majorPath), binding.toValue(usrs));
	}

	public void writeItemUsrs(String key, ItemUsrs itemUsrs) {
		List<String> majorPath = Arrays.asList("Sale");
		List<String> minorPath = Arrays.asList(key);
		// System.out.println("Key :" + majorPath.toString() + "-" + minorPath
		// + " Value :" + itemUsrs.toString());
		store.put(Key.createKey(majorPath, minorPath),
				binding.toValue(itemUsrs));
	}

	public void writeShop(String key, avro.Shop shop, avro.Items items)
			throws IOException {
		List<String> majorPath = Arrays.asList("Shop");
		List<String> minorPath = Arrays.asList(key, "name");

		Value value = Value.createValue(shop.getShopName().getBytes());
		store.put(Key.createKey(majorPath, minorPath), value);

		minorPath = Arrays.asList(key, "url");
		value = Value.createValue(shop.getUrl().getBytes());
		store.put(Key.createKey(majorPath, minorPath), value);

		if (items != null) {
			minorPath = Arrays.asList(key, "items");
			store.put(Key.createKey(majorPath, minorPath),
					binding.toValue(items));
		}
	}

	public void writeItem(String key, avro.Item item) {
		List<String> majorPath = Arrays.asList("Item");
		List<String> minorPath = Arrays.asList(key, "name");

		Value value = Value.createValue(item.getName().getBytes());
		store.put(Key.createKey(majorPath, minorPath), value);

		minorPath = Arrays.asList(key, "price");
		value = Value.createValue(Bytes.toBytes(item.getPrice()));
		store.put(Key.createKey(majorPath, minorPath), value);

		minorPath = Arrays.asList(key, "url");
		value = Value.createValue(item.getUrl().getBytes());
		store.put(Key.createKey(majorPath, minorPath), value);

		minorPath = Arrays.asList(key, "description");
		value = Value.createValue(item.getDescription().getBytes());
		store.put(Key.createKey(majorPath, minorPath), value);

		minorPath = Arrays.asList(key, "shopid");
		value = Value.createValue(item.getShopId().getBytes());
		store.put(Key.createKey(majorPath, minorPath), value);
	}

	public void writeOrder(String key, avro.Ordered ordered,
			avro.ItemLines itemLines) {
		List<String> majorPath = Arrays.asList("Order");
		List<String> minorPath = Arrays.asList(key, "date");

		Value value = Value.createValue(ordered.getDate().getBytes());
		store.put(Key.createKey(majorPath, minorPath), value);

		minorPath = Arrays.asList(key, "totalprice");
		value = Value.createValue(Bytes.toBytes(ordered.getTotalPrice()));
		store.put(Key.createKey(majorPath, minorPath), value);

		minorPath = Arrays.asList(key, "usrid");
		value = Value.createValue(ordered.getUsrId().getBytes());
		store.put(Key.createKey(majorPath, minorPath), value);

		minorPath = Arrays.asList(key, "itemline");
		// System.out.println(key + " " + itemLines.toString());
		store.put(Key.createKey(majorPath, minorPath),
				binding.toValue(itemLines));

	}

	public void writeOrderQuantity(String key, OrderQuantitys orderQuantitys) {
		List<String> majorPath = Arrays.asList("Item");
		List<String> minorPath = Arrays.asList(key, "orderquantity");
		store.put(Key.createKey(majorPath, minorPath),
				binding.toValue(orderQuantitys));
	}

	public static void main(String[] args) throws IOException {
		Conn2OracleNosql conn2OracleNosql = new Conn2OracleNosql();
		System.out.println("*** item ***");
		conn2OracleNosql.showAllItem();
		System.out.println("*** order ***");
		conn2OracleNosql.showAllOrder();
		System.out.println("*** shop ***");
		conn2OracleNosql.showAllShop();
		System.out.println("*** usr ***");
		conn2OracleNosql.showAllUsr();
		System.out.println("*** finished **");
	}

}
