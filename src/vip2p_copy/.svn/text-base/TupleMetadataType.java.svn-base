package vip2p_copy;

/**
 * Enumerated type representing the possible types of metadata in a tuple.
 * 
 * @author Jesus CAMACHO RODRIGUEZ
 *
 * @created 16/12/2010
 */
public enum TupleMetadataType {

    TUPLE_TYPE,
    STRING_TYPE,
    INTEGER_TYPE,
    NULL;
    
	/**
	 * Gets a String and returns the corresponding member of the enumerated type.
	 * @return the type that corresponds to the value passed to the method
	 */
	public static TupleMetadataType getTypeEnum(String stringType) {
		if (stringType.equals("TUPLE_TYPE"))
			return TUPLE_TYPE;
		else if (stringType.equals("STRING_TYPE"))
			return STRING_TYPE;
		else if (stringType.equals("INTEGER_TYPE"))
			return INTEGER_TYPE;
		else if (stringType.equals("null"))
			return NULL;
		else
			return null;
	}
	
	/**
	 * Returns the string representation of the current value of the enumerated type.
	 * @return string representation for the correspondent type
	 */
	public String toString() {
		switch(this) {
			case TUPLE_TYPE:
				return "TUPLE_TYPE";
			case STRING_TYPE:
				return "STRING_TYPE";
			case INTEGER_TYPE:
				return "INTEGER_TYPE";
			case NULL:
				return "null";
			default:
				return null;
		}
	}
}
