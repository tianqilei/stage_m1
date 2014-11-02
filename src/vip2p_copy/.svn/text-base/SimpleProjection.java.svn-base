package vip2p_copy;

/**
 * Physical operator that represents a simple projection over its child that
 * will keep only the columns specified in the constructor.
 * 
 * @author Ioana MANOLESCU
 */
public class SimpleProjection extends NIterator {

	private static final long serialVersionUID = -1877915260707327398L;

	int[] keepColumns;
	String projString;
	NTuple t;

	public NIterator child;

	public SimpleProjection(NIterator child, int keepColumn)
			throws VIP2PExecutionException {
		super(child);
		this.keepColumns = new int[1];
		keepColumns[0] = keepColumn;
		this.child = child;
		this.nrsmd = NRSMD.makeProjectRSMD(child.nrsmd, keepColumns);
		// this.nrsmd.display();

		// This will build new equivalence classes, perhaps taking into account
		// if a column ends up at more than one position.
		// If the 0-th group is not preserved, there will be no order.
		this.setOrderMaker(OrderMarker.project(child.getOrderMaker(),
				keepColumns));

		StringBuffer sb = new StringBuffer();
		sb.append("[");
		for (int i = 0; i < keepColumns.length; i++) {
			sb.append(keepColumns[i]);
			if (i < keepColumns.length - 1) {
				sb.append(", ");
			}
		}
		sb.append("]");
		projString = new String(sb);

		this.setOrderMaker(OrderMarker.project(child.getOrderMaker(),
				keepColumns));

		increaseOpNo();
	}

	public SimpleProjection(NIterator child, int[] keepColumns)
			throws VIP2PExecutionException {
		super(child);
		this.child = child;
		this.setPeer(this.child.getPeer());
		this.nrsmd = NRSMD.makeProjectRSMD(child.nrsmd, keepColumns);
		this.keepColumns = keepColumns;

		StringBuffer sb = new StringBuffer();
		sb = new StringBuffer();
		sb.append("[");
		for (int i = 0; i < keepColumns.length; i++) {
			sb.append(keepColumns[i]);
			if (i < keepColumns.length - 1) {
				sb.append(", ");
			}
		}
		sb.append("]");
		projString = new String(sb);

		// logger.debug("OrderMarker was: " + child.om.toString());
		// logger.debug("Projecting on: " + projString);
		this.setOrderMaker(OrderMarker.project(child.getOrderMaker(),
				keepColumns));
		// logger.debug("OrderMarker is: " + this.om.toString());

		increaseOpNo();

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see fr.inria.gemo.uload.execution.NIterator#open()
	 */
	public void open() throws VIP2PExecutionException {
		child.open();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see fr.inria.gemo.uload.execution.NIterator#hasNext()
	 */
	public boolean hasNext() throws VIP2PExecutionException {
		if (timeout) {
			return false;
		}

		return child.hasNext();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see fr.inria.gemo.uload.execution.NIterator#next()
	 */
	public NTuple next() throws VIP2PExecutionException {
		t = NTuple.project(child.next(), this.nrsmd, this.keepColumns);
		// logger.debug("Made: ");
		// t.display();
		this.numberOfTuples++;
		return t;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see fr.inria.gemo.uload.execution.NIterator#close()
	 */
	public void close() throws VIP2PExecutionException {
		child.close();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see fr.inria.gemo.uload.execution.NIterator#getNestedIterator(int)
	 */
	public NIterator getNestedIterator(int i) throws VIP2PExecutionException {
		return new ArrayIterator(t.getNestedField(i), t.nrsmd.getNestedChild(i));
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see fr.inria.gemo.uload.execution.NIterator#getName()
	 */
	@Override
	public String getName() {

		String tabs = getTabs(PRINTING_INDENTATION_TABS);

		return "SimpleProjection(" + child.getName(1) + "," + "\n" + tabs
				+ projString + "\n" + ")";
	}

	@Override
	public String getName(int depth) {

		String spaceForIndent = getTabs(PRINTING_INDENTATION_TABS * depth);
		String tabs = spaceForIndent + getTabs(PRINTING_INDENTATION_TABS);

		return "\n" + spaceForIndent + "SimpleProjection("
				+ child.getName(1 + depth) + "," + "\n" + tabs + projString
				+ "\n" + ")";
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see fr.inria.gemo.uload.execution.NIterator#copy()
	 */
	public NIterator copy() throws VIP2PExecutionException {
		return new SimpleProjection(this.child.copy(), this.keepColumns);
	}

	/**
	 * Adds the code for the graphical representation to the StringBuffer.
	 * 
	 * @author Aditya SOMANI
	 */
	public final int recursiveDotString(StringBuffer sb, int parentNo,
			int firstAvailableNo) {
		sb.append(firstAvailableNo + " [label=\"Project@"
				+ getPeer().toString());

		if (parentNo != -1) {
			sb.append(parentNo + " -> " + firstAvailableNo + " ; \n");
		}

		int childNumber1 = child.recursiveDotString(sb, firstAvailableNo,
				(firstAvailableNo + 1));
		return childNumber1;
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