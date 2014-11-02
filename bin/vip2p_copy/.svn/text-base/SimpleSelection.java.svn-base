package vip2p_copy;

/**
 * Physical operator that represents a simple selection over a set of tuples
 * coming from its child.
 * 
 * @author Ioana MANOLESCU
 */
public class SimpleSelection extends NIterator {

	private static final long serialVersionUID = -8883794766257452076L;

	NIterator child;
	Predicate pred;
	NTuple t;

	public SimpleSelection(NIterator child, Predicate pred) {
		super(child);
		this.child = child;
		this.pred = pred;
		this.nrsmd = child.nrsmd;

		this.setOrderMaker(child.getOrderMaker());

		increaseOpNo();
	}

	public void open() throws VIP2PExecutionException {
		child.open();
		this.t = null;
	}

	public boolean hasNext() throws VIP2PExecutionException {
		if (timeout) {
			return false;
		}

		while (true) {

			if (timeout) {
				return false;
			}

			if (child.hasNext()) {
				t = child.next();
				if (pred.isTrue(t)) {
					return true;
				}
			} else {
				return false;
			}
		}
	}

	public NTuple next() throws VIP2PExecutionException {
		this.numberOfTuples++;
		return t;
	}

	public void close() throws VIP2PExecutionException {
		child.close();
	}

	public NIterator getNestedIterator(int i) throws VIP2PExecutionException {
		return new ArrayIterator(t.getNestedField(i), t.nrsmd.getNestedChild(i));
	}

	@Override
	public String getName() {
		String predToString;

		if (pred == null)
			predToString = "null pred";
		else
			predToString = pred.getName();

		String tabs = getTabs(PRINTING_INDENTATION_TABS);

		return "SimpleSelection(" + child.getName(1) + "," + "\n" + tabs + "["
				+ predToString + "]" + "\n" + ")";
	}

	@Override
	public String getName(int depth) {
		String predToString;

		if (pred == null)
			predToString = "null pred";
		else
			predToString = pred.getName();

		String spaceForIndent = getTabs(PRINTING_INDENTATION_TABS * depth);
		String tabs = spaceForIndent + getTabs(PRINTING_INDENTATION_TABS);

		return "\n" + spaceForIndent + "SimpleSelection("
				+ child.getName(depth + 1) + "," + "\n" + tabs + "["
				+ predToString + "]" + "\n" + spaceForIndent + ")";
	}

	public NIterator copy() throws VIP2PExecutionException {
		return new SimpleSelection(this.child.copy(), this.pred);
	}

	// TODO finish this
	void estimateCost() {
		// Niterator child, Predicate pre
		// There are many type of predicate -> must consider all case
		// The reason why we should estimate the statistic in the logical
		// Logicaly. :-)
		SimplePredicate sp = (SimplePredicate) pred;
		if (sp.onString) {

		}

		if (sp.onJoin) {

		}
	}

	/**
	 * Adds the code for the graphical representation to the StringBuffer.
	 * 
	 * @author Aditya SOMANI
	 */
	public final int recursiveDotString(StringBuffer sb, int parentNo,
			int firstAvailableNo) {
		sb.append(firstAvailableNo + " [label=\"Filter@" + getPeer().toString()
				+ "\n");

		if (parentNo != -1) {
			sb.append(parentNo + " -> " + firstAvailableNo + " ; \n");
		}

		return child.recursiveDotString(sb, firstAvailableNo,
				(firstAvailableNo + 1));
	}

	/**
	 * Returns the number of NetReceive Operations under this class.
	 * 
	 * @author Aditya SOMANI
	 */
	public int receiveCount() {
		if (child == null)
			return 0;
		return child.receiveCount();
	}

}