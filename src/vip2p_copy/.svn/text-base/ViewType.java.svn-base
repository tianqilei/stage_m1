package vip2p_copy;

/**
 * Enumerated type representing the possible types of views that we can have in
 * the system.
 * 
 * @author Jesus CAMACHO RODRIGUEZ
 *
 * @created 21/04/2010
 */
public enum ViewType {
	COMPULSORY,
	COLLABORATIVE,
	SELFISH,
	CONTENT,
	ALL;
	
	/**
	 * Returns the string representation of the current value of the enumerated type for
	 * using it in the concatenation with the rest of the key for indexing in the DHT
	 * @return the string that will be used as a prefix for the key
	 */
	public String toStringForKey() {
		if (this == COMPULSORY)
			return "COMPULSORY";
		else if (this == COLLABORATIVE || this == SELFISH)
			return "";
		else if (this == CONTENT)
			return "CONTENT";
		else if (this == ALL)
			return "ALL";
		else
			return null;
	}
	
	/**
	 * Returns the string representation of the current value of the enumerated type
	 * @return the string representation for the type
	 */
	public String toString() {
		if (this == COMPULSORY)
			return "COMPUL";
		else if (this == COLLABORATIVE)
			return "COLLAB";
		else if (this == SELFISH)
			return "SELF";
		else if (this == CONTENT)
			return "CONTENT";
		else if (this == ALL)
			return "ALL";
		else
			return null;
	}
}
