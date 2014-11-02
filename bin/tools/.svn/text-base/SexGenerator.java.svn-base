package tools;

import java.io.IOException;
import java.util.Random;

public class SexGenerator {

	public static char getSex() {
		Random random = new Random();
		if (random.nextInt(2) == 0)
			return 'f';
		else
			return 'm';
	}
	
	public static void main(String[] args) throws IOException {
		for (int i = 0; i < 100; i++) {
			System.out.println(SexGenerator.getSex());
		}
	}

}
