package tools;

import dataGenerator.DataGeneratorIO;

/**
 * This class is to transform integer into same length for example : (if the
 * biggest number is 100) 2 -> 002 12 -> 012 100 -> 100
 */
public class TransformInteger {

	private int biggestInteger;
	private int i;

	public TransformInteger(int i, int biggestInteger) {
		this.biggestInteger = biggestInteger;
		this.i = i;
	}

	public static String transform(int i, int biggestInteger) {
		if (i == biggestInteger) {
			return Integer.toString(i);
		}

		if (i == 0) {
			String string = Integer.toString(biggestInteger);
			int compt = string.length();

			String res = "";
			for (int j = 0; j < compt; j++) {
				res = res.concat("0");
			}
			return res;
		}
		/*
		 * if (biggestInteger < i) {
		 * System.err.println("TransformInteger.java : the biggest integer :" +
		 * biggestInteger + " is smaller than the integer given :" + i); }
		 */
		int compt = 0;

		while (biggestInteger > i) {
			biggestInteger = biggestInteger / 10;
			compt++;
		}

		String res = "";
		for (int j = 0; j < compt; j++) {
			res = res.concat("0");
		}
		res = res.concat(Integer.toString(i));
		return res;
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		for (int i = 0; i < DataGeneratorIO.NB_ITEM; i++) {
			String itemIdString = "item" + i;

			GetIntFromId getIntFromId = new GetIntFromId(itemIdString, "item");
			String itemId = getIntFromId.restore(TransformInteger.transform(
					getIntFromId.get(), DataGeneratorIO.NB_ITEM));
			if (i != DataGeneratorIO.NB_ITEM) {
				String itemIdStopString = "item" + i + 1;
				getIntFromId = new GetIntFromId(itemIdStopString, "item");

				String itemIdStop = getIntFromId
						.restore(TransformInteger.transform(getIntFromId.get(),
								DataGeneratorIO.NB_ITEM));
				System.out.println(itemId + " " + itemIdStop);
			} else {

			}

		}
	}

}
