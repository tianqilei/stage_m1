package tools;

import java.text.SimpleDateFormat;

public class Millis2HHmmss {

	public static String get(long millis) {
		SimpleDateFormat formatter = new SimpleDateFormat("HH:mm:ss");
		return formatter.format(millis);
	}

	public static String formatLongToTimeStr(Long l) {
		int hour = 0;
		int minute = 0;
		int second = 0;
		int ms = 0;
		second = l.intValue() / 1000;
		ms = l.intValue() % 1000;
		if (second > 60) {
			minute = second / 60;
			second = second % 60;
		}
		if (minute > 60) {
			hour = minute / 60;
			minute = minute % 60;
		}
		return (Integer.toString(hour) + ":" + Integer.toString(minute) + ":"
				+ Integer.toString(second) + ":" + Integer.toString(ms));
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		String hString = Millis2HHmmss.formatLongToTimeStr((long) 1230);
		System.out.println(hString);
	}

}
