package tools;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Random;

/* 
 * This class is a dictionary reader :
 * 	we read from a dictionary "./tools/dictionary.txt" and add these word into an arrary of string 
 * */
public class DictionaryReader {

	private static final String DIC_PATH = "./tools/dictionary.txt";
	private int nbWord = 0;
	private ArrayList<String> words = new ArrayList<String>();

	public DictionaryReader() throws IOException {
		BufferedReader br = new BufferedReader(new InputStreamReader(
				new FileInputStream(DIC_PATH)));

		for (String line = br.readLine(); line != null; line = br.readLine()) {
			// System.out.println(line);
			words.add(line);
			nbWord++;
		}
		br.close();
	}

	public int getNbWord() {
		return nbWord;
	}

	/*
	 * generate one word randomly
	 */
	public String getRandomWord() {
		Random random = new Random();
		return words.get(random.nextInt(nbWord));
	}

	// generate n words randomly split by " " (space)
	public String getRandomWord(int n) {
		String string = "";

		if (n == 0) {
			return "";
		}
		string = getRandomWord();
		for (int i = 1; i < n; i++) {
			string = string.concat(" " + getRandomWord());
		}

		return string;
	}

	public static void main(String[] args) throws IOException {
		DictionaryReader dr = new DictionaryReader();
		System.out.println(dr.getNbWord());
		System.out.println(dr.getRandomWord(5));
	}

}
