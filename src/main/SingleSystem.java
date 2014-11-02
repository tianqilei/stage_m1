package main;

import dataGenerator.DataGeneratorIO;

public class SingleSystem {

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		DataGeneratorIO dataGeneratorIO = new DataGeneratorIO("my_data2",
				"postgres", "fenghuang");
		dataGeneratorIO.initData();
		System.out.println();
		System.out.println();
		System.out.println("Generating data ...");
		dataGeneratorIO.generateData();
		System.out.println("Generating data finished ...");
		System.out.println("Writing into Postgresql");

		dataGeneratorIO.writeUsers2Postgresql();
		System.out.println("user data writing into Postgresql finished ...");
		dataGeneratorIO.writeItem2Postgresql();
		System.out.println("item data writing into Postgresql finished ...");
		dataGeneratorIO.writeShops2Postgresql();
		System.out.println("shop data writing into Postgresql finished ...");
		dataGeneratorIO.writeAddress2Postgresql();
		System.out.println("address data writing into Postgresql finished ...");
		dataGeneratorIO.writeFriends2Postgresql();
		System.out.println("friend data writing into Postgresql finished ...");
		dataGeneratorIO.writeOrders2Postgresql();
		System.out.println("order data writing into Postgresql finished ...");
		dataGeneratorIO.writeVisit2Postgresql();
		System.out.println("visit data writing into Postgresql finished ...");
		dataGeneratorIO.writeLineItem2Postgresql();
		System.out
				.println("lineitem data writing into Postgresql finished ...");
		System.out.println();
		System.out.println();
		System.out.println("Writing into HBase");
		dataGeneratorIO.writeUsers2HBase();
		System.out.println("user data writing into HBase finished ...");
		dataGeneratorIO.writeVisit2HBase();
		System.out.println("visit data writing into HBase finished ...");
		dataGeneratorIO.writeFriendhip2HBase();
		System.out.println("friendship data writing into HBase finished ...");
		dataGeneratorIO.writeShops2HBase();
		System.out.println("shop data writing into HBase finished ...");
		dataGeneratorIO.writeItem2HBase();
		System.out.println("item data writing into HBase finished ...");
		dataGeneratorIO.writeOrder2HBase();
		System.out.println("order data writing into HBase finished ...");

		System.out.println();
		System.out.println();
		System.out.println("Write to Oracle Nosql");
		dataGeneratorIO.writeUsr2OracleNosql();
		System.out.println("user data writing into Oracle Nosql finished ...");
		dataGeneratorIO.writeShop2OracleNosql();
		System.out.println("shop data writing into Oracle Nosql finished ...");
		dataGeneratorIO.writeItem2OracleNosql();
		System.out.println("item data writing into Oracle Nosql finished ...");
		dataGeneratorIO.writeOrder2OracleNosql();
		System.out.println("order data writing into Oracle Nosql finished ...");
		dataGeneratorIO.writeCountryUsrs2OracleNosql();
		System.out
				.println("country-Users data writing into Oracle Nosql finished ...");
		dataGeneratorIO.writeItemUsrs2OracleNosql();
		System.out
				.println("item-users data writing into Oracle Nosql finished ...");
		dataGeneratorIO.writeOrderQuantity2OracleNosql();
		System.out
				.println("order-quantity data writing into Oracle Nosql finished ...");
		System.out.println("All finished!");

	}

}
