package vip2p_copy;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * @author Ioana MANOLESCU
 * @author Stamatis ZAMPETAKIS
 * 
 * @created 15/08/2005
 * @last-modified 13/12/2010
 */
public class MemoryHashJoin extends NIterator {

	private static final long serialVersionUID = 2516059726411005345L;

	/* Constants */
	private static final boolean USE_ONLY_JOIN_ON_VALUE = Boolean
			.parseBoolean(Parameters.getProperty("annovip.useOnlyJoinOnValue"));
	private static final boolean USE_FULL_NODE_ID = Boolean
			.parseBoolean(Parameters.getProperty("annovip.useFullNodeID"));

	protected NIterator left;
	protected NIterator right;
	protected Predicate pred;

	private HashMap<NTuple, ArrayList<NTuple>> hm;
	private HashMap<String, ArrayList<NTuple>> hmv;

	private NRSMD key1NRSMD;
	private NRSMD key2NRSMD;

	private NTuple tLeft;
	private NTuple tRight;

	// Arrays that hold the position of columns to make the joins
	private int[] leftJoinCols;
	private int[] rightJoinCols;

	private boolean overLeft;
	private boolean overRight;
	private ArrayList<NTuple> vRes;
	private int iRes;

	// private NTuple currentLeft;
	protected NTuple currentRes;

	private void createProjectionCols(NIterator left, NIterator right,
			Predicate p) {
		// Usually we have only two columns for each predicate but when we have
		// fullNodeIDs
		// joins then for each predicate we might need to project two columns
		ArrayList<Integer> tmpleftCols = new ArrayList<Integer>();
		ArrayList<Integer> tmpCols2 = new ArrayList<Integer>();
		int leftCol = -1;
		int rightCol = -1;
		if (p instanceof SimplePredicate) {
			SimplePredicate sp = (SimplePredicate) p;

			leftCol = sp.getColumn();
			/* question here */
			rightCol = sp.getOtherColumn() - left.nrsmd.colNo;
			// Commented it out since it consumes a lot of time
			// Parameters.logger.debug("Column Creation with <Simple Predicate>! "
			// + sp.toString());
			this.pred = sp;

			addAppropriateCols(tmpleftCols, left.nrsmd, leftCol);
			addAppropriateCols(tmpCols2, right.nrsmd, rightCol);

			if (sp.getPredCode() != PredicateType.PREDICATE_EQUAL) {
				throw new Error("HashJoin can only be used for equality joins");
			}

		} else {// pred should be a conjunctive predicate
			if (!(p instanceof ConjunctivePredicate)) {
				throw new Error(
						"HashJoin requires simple (or conjunctive) predicate, received "
								+ p.getClass().getName());
			}
			ConjunctivePredicate cp = (ConjunctivePredicate) p;

			// Parameters.logger.debug("Column Creation with <Conjunctive> predicate! "
			// + cp.toString());
			this.pred = cp;

			int k = cp.getPreds().length;

			for (int i = 0; i < k; i++) {
				Predicate predi = cp.getPreds()[i];
				try {
					SimplePredicate spredi = (SimplePredicate) predi;
					leftCol = spredi.getColumn();
					rightCol = spredi.getOtherColumn() - left.nrsmd.colNo;

					addAppropriateCols(tmpleftCols, left.nrsmd, leftCol);
					addAppropriateCols(tmpCols2, right.nrsmd, rightCol);

					if (spredi.getPredCode() != PredicateType.PREDICATE_EQUAL) {
						throw new Error(
								"HashJoin can only be used for equality joins");
					}

				} catch (ClassCastException cce3) {
					throw new Error("Unhandled predicate type "
							+ predi.getClass().getName());
				}
			}

		}

		leftJoinCols = new int[tmpleftCols.size()];
		// Parameters.logger.info("Left columns join of size: " +
		// leftJoinCols.length);
		for (int i = 0; i < tmpleftCols.size(); i++) {
			leftJoinCols[i] = tmpleftCols.get(i);
			// Parameters.logger.info(leftJoinCols[i]);
		}

		rightJoinCols = new int[tmpCols2.size()];
		// Parameters.logger.info("Right columns join of size: " +
		// rightJoinCols.length);
		for (int i = 0; i < tmpCols2.size(); i++) {
			rightJoinCols[i] = tmpCols2.get(i);
			// Parameters.logger.info(rightJoinCols[i]);
		}
	}

	/**
	 * Based on the nrsmd and the "col" add this column and all relative in the
	 * "columns" list
	 * 
	 * @param columns
	 *            - add the new columns inside here
	 * @param nrsmd
	 * @param col
	 * 
	 */
	private void addAppropriateCols(ArrayList<Integer> columns, NRSMD nrsmd,
			int col) {
		// If we use the full node ID then we migth need to project two columns
		// in every other case only one column is need to projected
		// Also there is no meaning using USE_FULL_NODE_ID without
		// USE_ONLY_JOIN_ON_VALUE
		if (USE_FULL_NODE_ID && USE_ONLY_JOIN_ON_VALUE
				&& matchIDType(nrsmd, col)) {
		//	columns.add(nrsmd.getRespectiveUriCol(col));
			columns.add(col);

		} else {
			columns.add(col);
		}

	}

	// Check if the nrmsd type of the iterator it in col is an ID
	private boolean matchIDType(NRSMD nrsmd, int col) {
		switch (nrsmd.getColumnsMetadata()[col]) {
		case STRING_TYPE:
			return false;
		default:
			throw new Error("Type unhandled by MemoryHashJoin "
					+ nrsmd.getColumnMetadata(col));
		}
	}

	public MemoryHashJoin(NIterator left, NIterator right, Predicate pred)
			throws VIP2PExecutionException {
		super(left, right);
		this.left = left;
		this.right = right;
		// Parameters.logger.info("MemoryHashJoin");

		createProjectionCols(left, right, pred);

		// ordering should be re-thought in the case of value joins. For the
		// moment we assume that
		// the order is that of the left child
		this.setOrderMaker(left.getOrderMaker());

		key1NRSMD = NRSMD.makeProjectRSMD(left.nrsmd, leftJoinCols);
		key2NRSMD = NRSMD.makeProjectRSMD(right.nrsmd, rightJoinCols);

		this.nrsmd = NRSMD.appendNRSMD(left.nrsmd, right.nrsmd);

	}

	public void open() throws VIP2PExecutionException {
		right.open();
		left.open();
		initialize();
	}

	@Override
	public void initialize() throws VIP2PExecutionException {

		// Parameters.logger.info("Initializing...");

		hm = new HashMap<NTuple, ArrayList<NTuple>>();
		hmv = new HashMap<String, ArrayList<NTuple>>();
		overLeft = false;
		overRight = false;

		NTuple keyRight = null;
		if (USE_ONLY_JOIN_ON_VALUE) {
			while (right.hasNext()) {

				if (timeout) {
					break;
				}

				tRight = right.next();
				// Parameters.logger.debug("Trying to project from the tuple: ");
				// tRight.display();
				// Parameters.logger.debug("on the fields: ");
				keyRight = NTuple.project(tRight, key2NRSMD, rightJoinCols);

				String[] strArray = keyRight.toStringArray();
				StringBuffer strb = new StringBuffer();
				for (int i = 0; i < strArray.length; i++)
					strb.append(strArray[i]);

				ArrayList<NTuple> v = hmv.get(strb.toString());
				if (v == null) {
					v = new ArrayList<NTuple>();
					v.add(tRight);
					hmv.put(strb.toString(), v);

					// Parameters.logger.info("Put 1 tuple on ");
					// keyRight.display();
					// Parameters.logger.info("There are " + hmv.keySet().size()
					// + " keys in hash map");
				} else {
					v.add(tRight);
				}

			}
		} else {
			while (right.hasNext()) {

				if (timeout) {
					break;
				}

				tRight = right.next();
				keyRight = NTuple.project(tRight, key2NRSMD, rightJoinCols);
				ArrayList<NTuple> v = hm.get(keyRight);
				if (v == null) {
					v = new ArrayList<NTuple>();
					v.add(tRight);
					hm.put(keyRight, v);
				} else {
					v.add(tRight);
				}
			}
		}
		overRight = true;
		vRes = new ArrayList<NTuple>();
		iRes = 0;
		// Parameters.logger.info("done!");
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

		if (overLeft && overRight) {
			return false;
		}
		if (vRes == null) {
			return false;
		}
		if (iRes < vRes.size()) {
			return true;
		} else {
			vRes = null;
			// nothing left for the current left tuple; moving to the next left
			// tuple

			NTuple keyLeft = null;
			if (USE_ONLY_JOIN_ON_VALUE) {
				// CONTINUOUS: MAKE SURE YOU RETAIN THE LOST TUPLE HERE. THE HAS
				// NEXT WAS CALLED PREVIOUSL ONCE. THE FIRST CALL SHOULD BE
				// TRUE.
				while (vRes == null && left.hasNext()) {

					if (timeout) {
						return false;
					}

					// CONTINUOUS: MAKE SURE YOU RETAIN THE LOST TUPLE HERE. ADD
					// A BOOLEAN TO CHECK IF THE FIRST WAS SKIPPED AND RETAINED
					tLeft = left.next();
					keyLeft = NTuple.project(tLeft, key1NRSMD, leftJoinCols);

					String[] strArray = keyLeft.toStringArray();
					StringBuffer strb = new StringBuffer();
					for (int i = 0; i < strArray.length; i++)
						strb.append(strArray[i]);

					vRes = hmv.get(strb.toString());
					// hm.remove(keyLeft);
				}
			} else {
				while (vRes == null && left.hasNext()) {

					if (timeout) {
						return false;
					}

					// CONTINUOUS: MAKE SURE YOU RETAIN THE LOST TUPLE HERE. ADD
					// A BOOLEAN TO CHECK IF THE FIRST WAS SKIPPED AND RETAINED
					tLeft = left.next();
					keyLeft = NTuple.project(tLeft, key1NRSMD, leftJoinCols);
					vRes = hm.get(keyLeft);
				}
			}
			if (vRes == null) {
				overRight = true;

				return false;
			} else {
				iRes = 0;
				return true;
			}
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see fr.inria.gemo.uload.execution.NIterator#next()
	 */
	public NTuple next() throws VIP2PExecutionException {
		NTuple nt = vRes.get(iRes);
		currentRes = NTuple.append(this.nrsmd, tLeft, nt);
		iRes++;
		this.numberOfTuples++;
		return currentRes;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see fr.inria.gemo.uload.execution.NIterator#close()
	 */
	public void close() throws VIP2PExecutionException {

		left.close();
		right.close();
		hmv = null;

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see fr.inria.gemo.uload.execution.NIterator#getNestedIterator(int)
	 */
	public NIterator getNestedIterator(int i) throws VIP2PExecutionException {
		return new ArrayIterator(currentRes.getNestedField(i),
				currentRes.nrsmd.getNestedChild(i));
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see fr.inria.gemo.uload.execution.NIterator#getName()
	 */

	@Override
	public String getName() {
		String predToString;

		if (pred == null)
			predToString = "null pred";
		else
			predToString = pred.getName();

		String tabs = getTabs(PRINTING_INDENTATION_TABS);

		return "MemoryHashJoin(" + left.getName(1) + "," + right.getName(1)
				+ "," + "\n" + tabs + "[" + predToString + "]" + "\n" + ")";
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

		return "\n" + spaceForIndent + "MemoryHashJoin("
				+ left.getName(1 + depth) + "," + right.getName(1 + depth)
				+ "," + "\n" + tabs + "[" + predToString + "]" + "\n"
				+ spaceForIndent + ")";
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see fr.inria.gemo.uload.execution.NIterator#copy()
	 */
	public NIterator copy() throws VIP2PExecutionException {
		return new MemoryHashJoin(this.left.copy(), this.right.copy(),
				this.pred);
	}

	/**
	 * Adds the code for the graphical representation to the StringBuffer.
	 * 
	 * @author Aditya SOMANI
	 */
	public final int recursiveDotString(StringBuffer sb, int parentNo,
			int firstAvailableNo) {
		sb.append(firstAvailableNo + " [label=\"HashJoin@"
				+ getPeer().toString());

		if (parentNo != -1) {
			sb.append(parentNo + " -> " + firstAvailableNo + " ; \n");
		}

		int childNumber1 = left.recursiveDotString(sb, firstAvailableNo,
				(firstAvailableNo + 1));
		int childNumber2 = right.recursiveDotString(sb, firstAvailableNo,
				childNumber1);

		return childNumber2;
	}

	/**
	 * Returns the number of NetReceive Operations under this class.
	 * 
	 * @author Aditya SOMANI
	 */
	public int receiveCount() {
		if (left != null && right != null)
			return (left.receiveCount() + right.receiveCount());
		if (left != null && right == null)
			return left.receiveCount();
		if (left == null && right != null)
			return right.receiveCount();
		return 0;
	}

	/**
	 * 
	 * @return an array of the columns that are used for the join in the left
	 *         NIterator
	 */
	public int[] getLeftJoinCols() {
		return leftJoinCols;
	}

	/**
	 * @return an array of the columns that are used for the join in the right
	 *         NIterator
	 * 
	 */
	public int[] getRightJoinCols() {
		return rightJoinCols;
	}

	/**
	 * 
	 * @return the Predicate of the MemoryHashJoin
	 */
	public Predicate getPredicate() {
		return pred;
	}

}
