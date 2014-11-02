package tools;
/**
 * This class is to get the integer from id
 * for example : 
 * shop15 -> 15
 * usr4 -> 4  
 */
public class GetIntFromId {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		GetIntFromId getIntFromId = new GetIntFromId("usr6", "usr");
		int i = getIntFromId.get();
		String string = getIntFromId.restore("006");
		System.out.println(i);
		System.out.println(string);
	}
	
	private String id;
	private String stringToDelete;
	
	public GetIntFromId(String id, String stringToDelete) {
		this.id = id;
		this.stringToDelete = stringToDelete;
	}
	
	public int get() {
		int i = stringToDelete.length();
		String resString = id.substring(i);
		return Integer.parseInt(resString);
	}

	public String restore(String newIntegerString) {
		String resString = "";
		resString = resString.concat(stringToDelete);
		resString = resString.concat(newIntegerString);
		return resString;
	}
}
