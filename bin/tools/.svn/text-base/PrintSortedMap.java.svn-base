package tools;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

public class PrintSortedMap {

	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

	public static void printf(HashMap<String, Integer> hashMap, int limit) {
		if (!hashMap.isEmpty() && hashMap != null) {

			ArrayList<Map.Entry<String, Integer>> l = new ArrayList<Map.Entry<String, Integer>>(
					hashMap.entrySet());

			Collections.sort(l, new Comparator<Map.Entry<String, Integer>>() {
				public int compare(Map.Entry<String, Integer> o1,
						Map.Entry<String, Integer> o2) {
					return (o2.getValue() - o1.getValue());
				}
			});

			for (int i = 0; i < limit; i++) {
				Entry<String, Integer> entry = l.get(i);
				System.out
						.println(entry.getKey() + " nb : " + entry.getValue());
			}
		} else {
			System.err.println("PrintSortedMap : HashMap is nul or empty");
		}
	}

	public static ArrayList<Entry<String, Integer>> getSortedList(
			HashMap<String, Integer> hashMap, int limit) {
		ArrayList<Entry<String, Integer>> newList = new ArrayList<Map.Entry<String, Integer>>();
		ArrayList<Map.Entry<String, Integer>> l = new ArrayList<Map.Entry<String, Integer>>(
				hashMap.entrySet());

		Collections.sort(l, new Comparator<Map.Entry<String, Integer>>() {
			public int compare(Map.Entry<String, Integer> o1,
					Map.Entry<String, Integer> o2) {
				return (o2.getValue() - o1.getValue());
			}
		});
		for (int i = 0; i < limit; i++) {
			Entry<String, Integer> entry = l.get(i);
			newList.add(entry);
		}
		return newList;
	}

}
