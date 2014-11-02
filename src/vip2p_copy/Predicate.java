package vip2p_copy;

import java.util.ArrayList;

/**
 * Abstract class that any predicate should extend.
 * 
 * @author Ioana MANOLESCU
 */
public abstract class Predicate implements Cloneable {
	
	public abstract boolean isTrue(NTuple t) throws VIP2PExecutionException;
	
	public abstract String toString();
	
	//For getting string representation of predicates separated by commas as in Plan file grammar
	public abstract String getName();
	
	/* (non-Javadoc)
	 * @see java.lang.Object#clone()
	 */
	public abstract Object clone();
	
	/**
	 * add an integer offset to the columns to which a predicate refers
	 * @author Konstantinos KARANASOS
	 */
	public static void addOffsetToPredicate(Predicate pred, int offset) {
		if (pred instanceof SimplePredicate) {
			((SimplePredicate) pred).column += offset;
			if (((SimplePredicate) pred).onJoin)
				((SimplePredicate) pred).otherColumn += offset;
		}
		else if (pred instanceof ConjunctivePredicate)
			for (int i = 0; i < ((ConjunctivePredicate) pred).preds.length; i++)
				addOffsetToPredicate(((ConjunctivePredicate) pred).preds[i], offset);
	}
	
	/**
	 * gets a Predicate (either Simple or Conjunctive) and a SimplePredicates list, extracts the 
	 * SimplePredicates from the Predicate and adds them to the list
	 * @author Konstantinos KARANASOS
	 */
	public static void addPredToSimplePredList(Predicate pred, ArrayList<SimplePredicate> simplePredsList) {
		if (pred instanceof SimplePredicate)
			simplePredsList.add((SimplePredicate) pred);
		else if (pred instanceof ConjunctivePredicate)
			for (int i = 0; i < ((ConjunctivePredicate) pred).preds.length; i++)
				addPredToSimplePredList(((ConjunctivePredicate) pred).preds[i], simplePredsList);
	}
}