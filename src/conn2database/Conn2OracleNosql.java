package conn2database;

/*-
 *
 *  This file is part of Oracle NoSQL Database
 *  Copyright (C) 2011, 2014 Oracle and/or its affiliates.  All rights reserved.
 *
 *  Oracle NoSQL Database is free software: you can redistribute it and/or
 *  modify it under the terms of the GNU Affero General Public License
 *  as published by the Free Software Foundation, version 3.
 *
 *  Oracle NoSQL Database is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 *  Affero General Public License for more details.
 *
 *  You should have received a copy of the GNU Affero General Public
 *  License in the LICENSE file along with Oracle NoSQL Database.  If not,
 *  see <http://www.gnu.org/licenses/>.
 *
 *  An active Oracle commercial licensing agreement for this product
 *  supercedes this license.
 *
 *  For more information please contact:
 *
 *  Vice President Legal, Development
 *  Oracle America, Inc.
 *  5OP-10
 *  500 Oracle Parkway
 *  Redwood Shores, CA 94065
 *
 *  or
 *
 *  berkeleydb-info_us@oracle.com
 *
 *  [This line intentionally left blank.]
 *  [This line intentionally left blank.]
 *  [This line intentionally left blank.]
 *  [This line intentionally left blank.]
 *  [This line intentionally left blank.]
 *  [This line intentionally left blank.]
 *  EOF
 *
 */

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.avro.specific.SpecificRecord;

import avro.Address;
import avro.Usr;
import avro.Visit;
import oracle.kv.Direction;
import oracle.kv.KVStore;
import oracle.kv.KVStoreConfig;
import oracle.kv.KVStoreFactory;
import oracle.kv.Key;
import oracle.kv.KeyValueVersion;
import oracle.kv.Value;
import oracle.kv.ValueVersion;
import oracle.kv.avro.AvroCatalog;
import oracle.kv.avro.SpecificAvroBinding;
import tools.IPGetter;

/*
 * create specific binding by using command : ant
 * put the schema and generate-specific.xml in the same directory 
 * and run : 
 * ant -f generate-specific.xml
 * then the new java file appear at ../avro
 * */
public class Conn2OracleNosql {

	private final KVStore store;
	private static final String HOST_NAME = IPGetter.get();
	private static final String HOST_PORT = "5000";

	private final AvroCatalog catalog;
	private final SpecificAvroBinding<SpecificRecord> binding;

	/**
	 * Runs the HelloBigDataWorld command line program.
	 * 
	 * @throws IOException
	 */
	public static void main(String args[]) throws IOException {
		try {
			Conn2OracleNosql example = new Conn2OracleNosql();
			example.showAllUsr();
			;
		} catch (RuntimeException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Parses command line args and opens the KVStore.
	 * 
	 * @throws IOException
	 */
	public Conn2OracleNosql() throws IOException {

		String storeName = "kvstore";
		String hostName = HOST_NAME;
		String hostPort = HOST_PORT;

		store = KVStoreFactory.getStore(new KVStoreConfig(storeName, hostName
				+ ":" + hostPort));

		catalog = store.getAvroCatalog();
		binding = catalog.getSpecificMultiBinding();
	}

	public SpecificAvroBinding<SpecificRecord> getBinding() {
		return binding;
	}

	public KVStore getStore() {
		return store;
	}

	public String getString(Key key) {
		ValueVersion vv2 = store.get(key);
		if (vv2 != null) {
			Value value = vv2.getValue();
			byte[] bytes = value.getValue();
			return new String(bytes);
		}
		return null;
	}

	public ValueVersion getValueVersion(Key key) {
		return store.get(key);
	}

	public int bytes2Int(byte[] bytes) {
		ByteBuffer bb = ByteBuffer.wrap(bytes);
		return bb.getInt();
	}

	public int getInteger(Key key) {
		ValueVersion vv2 = store.get(key);
		Value value = vv2.getValue();
		byte[] bytes = value.getValue();
		return bytes2Int(bytes);
	}

	public double getDouble(Key key) {
		ValueVersion vv2 = store.get(key);
		if (vv2 == null) {
			return 0;
		}
		Value value = vv2.getValue();
		byte[] bytes = value.getValue();
		return ByteBuffer.wrap(bytes).getDouble();
	}

	public SpecificRecord get(Key key) {
		ValueVersion vv = store.get(key);
		return binding.toObject(vv.getValue());
	}

	public SpecificRecord getRecord(Key key) {
		ValueVersion vv = store.get(key);

		if (!(vv == null)) {
			return binding.toObject(vv.getValue());
		}

		return null;
	}

	/*
	 * Key should be a part of majorKey
	 */
	public ArrayList<SpecificRecord> getRecords(Key key) {
		ArrayList<SpecificRecord> specificRecords = new ArrayList<SpecificRecord>();
		Iterator<KeyValueVersion> i = store.storeIterator(Direction.UNORDERED,
				0, key, null, null);
		while (i.hasNext()) {
			// Value v = i.next().getValue();
			KeyValueVersion kvv = i.next();
			SpecificRecord specificRecord = binding.toObject(kvv.getValue());
			specificRecords.add(specificRecord);
		}
		return specificRecords;
	}

	public ArrayList<avro.Usr> getUsrList(Key key) {
		ArrayList<avro.Usr> usrs = new ArrayList<avro.Usr>();
		Iterator<KeyValueVersion> i = store.storeIterator(Direction.UNORDERED,
				0, key, null, null);
		while (i.hasNext()) {
			// Value v = i.next().getValue();
			KeyValueVersion kvv = i.next();
			SpecificRecord specificRecord = binding.toObject(kvv.getValue());
			usrs.add((Usr) specificRecord);
		}
		return usrs;
	}

	public void showAllUsr() {
		ArrayList<String> majorPath = new ArrayList<String>();
		majorPath.add("Usr");
		Key key = Key.createKey(majorPath);
		ArrayList<SpecificRecord> srs = getRecords(key);
		System.out.println("All Users :" + srs.size());

		Iterator<SpecificRecord> i = srs.iterator();
		while (i.hasNext()) {
			SpecificRecord specificRecord = i.next();
			avro.Usr usr = (avro.Usr) specificRecord;
			System.out.println(usr.toString());
		}
		System.out.println();
		System.out.println();
	}

	public void showAllItem() {
		ArrayList<String> majorPath = new ArrayList<String>();
		majorPath.add("Item");
		Key key = Key.createKey(majorPath);
		ArrayList<SpecificRecord> srs = getRecords(key);
		System.out.println("All items : " + srs.size());

		Iterator<SpecificRecord> i = srs.iterator();
		while (i.hasNext()) {
			SpecificRecord specificRecord = i.next();
			avro.Item item = (avro.Item) specificRecord;
			System.out.println(item.toString());
		}
		System.out.println();
		System.out.println();
	}

	public void showAllShop() {
		ArrayList<String> majorPath = new ArrayList<String>();
		majorPath.add("Shop");
		Key key = Key.createKey(majorPath);
		ArrayList<SpecificRecord> srs = getRecords(key);
		System.out.println("All shops :" + srs.size());

		Iterator<SpecificRecord> i = srs.iterator();
		while (i.hasNext()) {
			SpecificRecord specificRecord = i.next();
			avro.Shop shop = (avro.Shop) specificRecord;
			System.out.println(shop.toString());
		}
		System.out.println();
		System.out.println();
	}

	public void showAllOrder() {
		ArrayList<String> majorPath = new ArrayList<String>();
		majorPath.add("Order");
		Key key = Key.createKey(majorPath);
		ArrayList<SpecificRecord> srs = getRecords(key);
		System.out.println("All orders : " + srs.size());

		Iterator<SpecificRecord> i = srs.iterator();
		while (i.hasNext()) {
			SpecificRecord specificRecord = i.next();
			avro.Ordered ordered = (avro.Ordered) specificRecord;
			System.out.println(ordered.toString());
		}
		System.out.println();
		System.out.println();
	}

	public void runExample2() {
		avro.Usr usr = new Usr();
		usr.setAge(15);
		usr.setName("test");
		usr.setSex("f");
		ArrayList<String> list = new ArrayList<String>();
		ArrayList<Address> addrs = new ArrayList<Address>();
		ArrayList<Visit> visits = new ArrayList<Visit>();

		list.add("111");
		list.add("222");

		Address address = new Address();
		address.setCity("c1");
		address.setStreet("s1");
		address.setCountry("state1");
		addrs.add(address);

		Visit visit = new Visit();
		visit.setBuy("y");
		visit.setDate("qsdfq");
		visit.setItemId("item1");
		visit.setTime("qdsfq");

		visits.add(visit);
		/*
		 * usr.setAddress(addrs); usr.setFriendship(list); usr.setVisit(visits);
		 * store.put(Key.createKey("usr0"), binding.toValue(usr)); ValueVersion
		 * vv = store.get(Key.createKey("usr0")); SpecificRecord u =
		 * binding.toObject(vv.getValue()); Usr tUsr = (Usr) u;
		 * System.out.println(tUsr.getName());
		 * System.out.println(tUsr.getFriendship().toString());
		 * System.out.println(tUsr.getAddress().toString());
		 * System.out.println(tUsr.getVisit().toString());
		 */
	}

	public void usage(String message) {
		System.out.println("\n" + message + "\n");
		System.out.println("usage: " + getClass().getName());
		System.out.println("\t-store <instance name> (default: kvstore) "
				+ "-host <host name> (default: localhost) "
				+ "-port <port number> (default: 5000)");
		System.exit(1);
	}

	/**
	 * Performs example operations and closes the KVStore.
	 */
	void runExample() {

		final String keyString = "Hello";
		final String valueString = "Big Data World!";

		store.put(Key.createKey(keyString),
				Value.createValue(valueString.getBytes()));

		final ValueVersion valueVersion = store.get(Key.createKey(keyString));
		System.out.println(keyString + " "
				+ new String(valueVersion.getValue().getValue()));

		store.close();
	}
}
