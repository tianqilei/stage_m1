package main;

import dataGenerator.DataGeneratorIO;

public class MultiSystem {

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
		System.out.println();
		System.out.println("Writing into Postgresql ...");
		dataGeneratorIO.writeItem2Postgresql();
		System.out.println("item data writing into Postgresql finished ...");
		System.out.println("Writing into HBase ...");
		dataGeneratorIO.writeShops2HBase();
		System.out.println("shop data writing into HBase finished ...");
		System.out.println("finished ...");
	}

}
