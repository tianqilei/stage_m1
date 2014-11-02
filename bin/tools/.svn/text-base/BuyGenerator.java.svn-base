package tools;

import java.util.Random;

public class BuyGenerator {

	public static char getBuy() {
		Random random = new Random();
		if (random.nextInt(2) == 0)
			return 'y';
		else
			return 'n';
	}

	public static void main(String[] args) {
		for (int i = 0; i < 100; i++) {
			System.out.println(BuyGenerator.getBuy());
		}
	}

}
