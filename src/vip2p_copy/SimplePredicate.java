package vip2p_copy;

import java.io.Serializable;
import java.util.Arrays;

/**
 * This class is meant for very SimplePredicate selections: atomic field =
 * constant, or atomic field = atomic field.
 * 
 * @author Ioana MANOLESCU
 */
public class SimplePredicate extends Predicate implements Serializable {

	private static final long serialVersionUID = -1282419749973424931L;

	public char[] theChars; // = new char[0];

	public boolean onString;
	public boolean onJoin;

	/**
	 * the column to which the selection is applied
	 */
	public int column;
	public int otherColumn;
	public int[] ancPath; // = new int[0];
	String ancPathString; // = new String("");

	public PredicateType predCode = PredicateType.PREDICATE_EQUAL;

	public SimplePredicate(String s, int k, PredicateType predCode) {
		this(s, k);
		this.predCode = predCode;
	}

	public SimplePredicate(String s, int k) {
		this.column = k;
		this.theChars = s.toCharArray();
		this.onString = true;
		this.onJoin = false;
		this.predCode = PredicateType.PREDICATE_EQUAL;
	}

	public SimplePredicate(char[] c, int k) {
		this.column = k;
		this.theChars = c;
		this.onString = true;
		this.onJoin = false;
		this.predCode = PredicateType.PREDICATE_EQUAL;
	}

	public SimplePredicate(char[] c, int k, PredicateType predCode) {
		this(c, k);
		this.predCode = predCode;
	}

	public SimplePredicate(int k1, int k2) {
		this.column = k1;
		this.ancPath = new int[1];
		this.ancPath[0] = k1;
		this.otherColumn = k2;
		this.onString = false;
		this.onJoin = true;
		this.predCode = PredicateType.PREDICATE_EQUAL;
	}

	public SimplePredicate(int k1, int k2, PredicateType predCode) {
		this(k1, k2);
		this.predCode = predCode;
	}

	// this means: the field reachable by k1[0].k1[1]. ... k1[nk1] is compared
	// with
	// the field reachable at k2
	// both have to be atomic
	public SimplePredicate(int[] k1, int k2) {
		this(k1, k2, PredicateType.PREDICATE_EQUAL);
	}

	public SimplePredicate(int[] k1, int k2, PredicateType predCode) {
		this.ancPath = k1;
		if (this.ancPath.length == 1) {
			this.column = k1[0];
		}
		this.otherColumn = k2;
		this.onString = false;
		this.onJoin = true;
		this.predCode = predCode;
		setAncPathString();
	}

	// this means: the field reachable by k1[0].k1[1]. ... k1[nk1] is compared
	// with
	// the field reachable at k2
	// both have to be atomic
	public SimplePredicate(int[] k1, String constValue) {
		this(k1, constValue, PredicateType.PREDICATE_EQUAL);
	}

	public SimplePredicate(int[] k1, String constValue, PredicateType predCode) {
		this.ancPath = k1;
		this.column = -1;
		this.otherColumn = -1;
		this.theChars = constValue.toCharArray();
		this.onString = true;
		this.onJoin = false;
		this.predCode = predCode;
		setAncPathString();
	}

	/**
	 * 
	 * @return the column of the predicate
	 */
	public int getColumn() {
		return column;
	}

	/**
	 * 
	 * @return the otherColumn of the predicate
	 */
	public int getOtherColumn() {
		return otherColumn;
	}

	/**
	 * 
	 * @return the predCode of the predicate
	 */
	public PredicateType getPredCode() {
		return predCode;
	}

	/**
	 * 
	 */
	private void setAncPathString() {
		StringBuffer sb = new StringBuffer();
		sb.append("[");
		for (int i = 0; i < ancPath.length; i++) {
			sb.append(ancPath[i]);
			if (i < ancPath.length - 1) {
				sb.append(".");
			}
		}
		sb.append("]");
		this.ancPathString = new String(sb);
	}

	/*
	 * (non-Javadoc) !!! Caution: this never works if ancPath is used (and
	 * this.column is not filled in). Such predicates should never be actually
	 * tested.
	 * 
	 * @see
	 * fr.inria.gemo.uload.execution.Predicate#isTrue(fr.inria.gemo.uload.execution
	 * .NTuple)
	 */
	public boolean isTrue(NTuple t) throws VIP2PExecutionException {
		assert (this.column >= 0) : "Cannot test the predicate ! Use GeneralizedSelect instead.";
		if (this.onString) {
			switch (this.predCode) {
			case PREDICATE_EQUAL:
				return equal(t.getStringField(this.column), this.theChars);
			case PREDICATE_NOTEQUAL:
				return (!equal(t.getStringField(this.column), this.theChars));
			default:
				System.err.println("Predicate " + this.predCode.toString()
						+ " not applicable to type String");
				// Parameters.logger.error("Predicate " +
				// this.predCode.toString() +
				// " not applicable to type String");
			}
		} else if (this.onJoin) {
			switch (t.nrsmd.types[this.column]) {
			case STRING_TYPE:
				switch (this.predCode) {
				case PREDICATE_EQUAL:
					return equal(t.getStringField(this.column),
							t.getStringField(this.otherColumn));
				case PREDICATE_NOTEQUAL:
					return !equal(t.getStringField(this.column),
							t.getStringField(this.otherColumn));
				default:
					System.err.println("Predicate " + this.predCode.toString()
							+ " not handled in String join");
					// Parameters.logger.error("Predicate " +
					// this.predCode.toString() +
					// " not handled in String join");
				}

			default:
				System.err.println("Type unhandled by SimplePredicates "
						+ t.nrsmd.types[this.column].toString());
				// Parameters.logger.error("Type unhandled by SimplePredicates "
				// + t.nrsmd.types[this.column].toString());
			}
		}
		return false;
	}

	public static boolean equal(char[] aux1, char[] aux2) {
		if (aux1.length != aux2.length) {
			return false;
		}
		for (int j = 0; j < aux1.length; j++) {
			if (aux1[j] != aux2[j]) {
				return false;
			}
		}
		return true;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see fr.inria.gemo.uload.execution.Predicate#toString()
	 */
	public String toString() {
		if (this.onJoin) {
			return (((this.column != -1) ? (this.column + "")
					: this.ancPathString) + this.predCode.toString() + this.otherColumn);
		} else {
			if (this.onString) {
				return (((this.column != -1) ? (this.column + "")
						: this.ancPathString)
						+ this.predCode.toString()
						+ "\""
						+ new String(this.theChars) + "\"");
			}
		}
		return "";
	}

	@Override
	public boolean equals(Object spred) {
		SimplePredicate pred = (SimplePredicate) spred;

		if (Arrays.equals(theChars, pred.theChars)
				&& onString == pred.onString
				&& onJoin == pred.onJoin
				&& ((ancPath == null && pred.ancPath == null) || Arrays.equals(
						ancPath, pred.ancPath))
				&& ((ancPathString == null && pred.ancPathString == null) || (ancPathString != null && ancPathString
						.equals(pred.ancPathString))) && predEquals(pred))
			return true;

		return false;
	}

	private boolean predEquals(SimplePredicate pred) {
		return (column == pred.column && otherColumn == pred.otherColumn && predCode
				.equals(pred.predCode))

				||

				(column == pred.otherColumn && otherColumn == pred.column && predCode
						.equals(pred.predCode.revert()));
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see fr.inria.gemo.vip2p.predicates.Predicate#clone()
	 */
	@Override
	public Object clone() {
		// TODO: add support for ancPath?
		if (onJoin) {
			return new SimplePredicate(column, otherColumn, this.predCode);
		} else if (onString) {
			return new SimplePredicate(theChars, column);
		} else {
			return null;
		}
	}

	/**
	 * @author Karan AGGARWAL
	 * 
	 * @return String representation of Predicate separated by , Follows the
	 *         Plan file (.phyp) grammar
	 */
	public String getName() {

		if (this.onJoin) {
			if (this.column != -1)
				return this.column + this.predCode.toString()
						+ this.otherColumn;

			else
				return this.ancPathString + this.predCode.toString()
						+ this.otherColumn;

		} else {

			if (this.onString) {
				if (this.column != -1)
					return new String(this.theChars) + this.column
							+ this.predCode.toString() + this.column;

				else
					return new String(this.theChars) + this.ancPathString
							+ this.predCode.toString() + this.column;
			}
		}

		return "";

	}

}