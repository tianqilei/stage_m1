package vip2p_copy;

/**
 * Enumerated type representing the possible types of predicates.
 * 
 * @author Jesus CAMACHO RODRIGUEZ
 *
 * @created 16/12/2010
 */
public enum PredicateType {
	PREDICATE_EQUAL,
	PREDICATE_NOTEQUAL,
	PREDICATE_PARENT,
	PREDICATE_CHILD,
	PREDICATE_ANCESTOR,
	PREDICATE_DESCENDANT,
	PREDICATE_BEFORE;
	
	/**
	 * Returns the string representation of the current value of the enumerated type.
	 * @return the string representation for this predicate
	 */
	public String toString() {
		switch(this) {
			case PREDICATE_EQUAL:
				return "=";
			case PREDICATE_NOTEQUAL:
				return "!=";
			case PREDICATE_ANCESTOR:
				return "<<";
			case PREDICATE_PARENT:
				return "<";
			case PREDICATE_BEFORE:
				return " before ";
			case PREDICATE_CHILD:
				return ">";
			case PREDICATE_DESCENDANT:
				return ">>";
			default:
				return null;
		}
	}
	
	/**
	 * Returns the string representation for debugging purposes (more clear than the
	 * representation returned by {@link #toString()} method) of the current value
	 * of the enumerated type.
	 * @return the debugging string representation for this predicate
	 */
	public String toDebugString(){
		switch(this) {
			case PREDICATE_EQUAL:
				return "=";
			case PREDICATE_NOTEQUAL:
				return "!=";
			case PREDICATE_ANCESTOR:
				return " ancestor of ";
			case PREDICATE_PARENT:
				return " parent of ";
			case PREDICATE_BEFORE:
				return " before ";
			case PREDICATE_CHILD:
				return " child of";
			case PREDICATE_DESCENDANT:
				return " descendant of ";
			default:
				return null;
		}
	}

	/**
	 * Returns the reverse type for this predicate. If this predicate does not
	 * have a reverse, then the method returns the same predicate.
	 * @return the reverse type
	 */
	public PredicateType revert() {
		switch (this) {
			case PREDICATE_PARENT:
				return PredicateType.PREDICATE_CHILD;
			case PREDICATE_CHILD:
				return PredicateType.PREDICATE_PARENT;
			case PREDICATE_ANCESTOR:
				return PredicateType.PREDICATE_DESCENDANT;
			case PREDICATE_DESCENDANT:
				return PredicateType.PREDICATE_ANCESTOR;
		}
		return this;	
	}
	
	
	
	
}
