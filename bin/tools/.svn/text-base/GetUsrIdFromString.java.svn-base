package tools;

/**
 * get usrId from a string (this class used in Query4HBase.java)
 * 
 * for example : item41/usr2/2012-09-06/23:34:45 -> usr2
 * 
 * */
public class GetUsrIdFromString {

	public static String getUsrId(String s) {
		if (s == null || s.isEmpty()) {
			System.err
					.println("GetUsrIdFromString.java error : string given is null or empty!");
			return null;
		}

		String[] ss = s.split("/");
		return ss[1];
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		String string = "item41/usr2/2012-09-06/23:34:45";
		System.out.println(getUsrId(string));
	}

}
