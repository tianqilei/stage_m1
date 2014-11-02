package tools;

import java.util.Random;

/*
 * generate a price for items 
 * the max price is MAX_PRICE
 * */
public class PriceGenerator {

	private static final int MAX_PRICE = 100000;

	public static float getPrice() {
		Random random = new Random();
		return IntGenerator.getInt(MAX_PRICE) - random.nextFloat();
	}

	public static void main(String[] args) {
		for (int i = 0; i < 100; i++) {
			System.out.println(PriceGenerator.getPrice());
		}
	}

}
