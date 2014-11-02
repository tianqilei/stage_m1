package tools;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class IntersectionOfList {
	public static <T> List<T> getResult(List<T> list1, List<T> list2) {
		List<T> list = new ArrayList<T>();

		for (T t : list1) {
			if (list2.contains(t)) {
				list.add(t);
			}
		}

		return list;
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		ArrayList<String> list = new ArrayList<String>();
		ArrayList<String> list2 = new ArrayList<String>();
		list.add("1");
		list.add("2");
		list.add("4");
		list.add("6");
		list.add("7");
		list2.add("1");
		list2.add("5");
		list2.add("10");
		list.retainAll(list2);
		System.out.println(list.size());
	}

}
