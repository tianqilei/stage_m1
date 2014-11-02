package tools;

public class UrlGenerator {

	private static final String DN = "http://A.B.C/";

	public static String getUrl(String id) {
		return DN.concat(id);
	}

	public static void main(String[] args) {
		System.out.println(UrlGenerator.getUrl("qdsfqdfd134124"));
	}

}
