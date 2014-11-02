package tools;

import java.net.InetAddress;
import java.net.UnknownHostException;

public class IPGetter {

	public static String get() {
		// TODO Auto-generated constructor stub
		InetAddress addr = null;
		try {
			addr = InetAddress.getLocalHost();
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return addr.getHostAddress();
	}
	
	public static void main(String[] args) throws UnknownHostException {
		// TODO Auto-generated method stub
		System.out.println(IPGetter.get());
	}

}
