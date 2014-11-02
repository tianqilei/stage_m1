package tools;

public class TimeGettor {

	public static long get() {
		return System.currentTimeMillis();
	}
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		long i1 = TimeGettor.get();
		int j=0;
		for (int i = 0; i < 10000; i++) {
			System.out.println(j++);
		}
		
		long i2 = TimeGettor.get();
		System.out.println(i2 - i1);
	}

}
