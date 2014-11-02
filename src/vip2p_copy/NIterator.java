package vip2p_copy;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

/**
 * Abstract class for every physical operator should extend.
 * 
 * @author Ioana MANOLESCU
 * @author Spyros ZOUPANOS
 * @author Aditya SOMANI
 */
public abstract class NIterator implements Serializable {

	public static boolean timeout = false;

	private static final long serialVersionUID = -2931824780181255334L;

	/* Constant */
	// Here we store if the physical plan monitor is active or not.
	private static final boolean PHYSICAL_MONITOR_ACTIVE = false;
	// Parameters.getProperty("physPlanMon").equals("yes");

	/**
	 * The list of children, to enable tree navigation
	 */
	public List<NIterator> children;

	/**
	 * Global counter to determine whether we create too many objects
	 */
	public static long totalOperatorNo = 0;

	/**
	 * Here we keep the number assigned to this operator
	 */

	public NRSMD nrsmd;

	/**
	 * The columns by which the output of this operator is ordered. The
	 * OrderMarker comprises:
	 * 
	 * -- a list of direct order-by columns, which are top-level columns
	 * 
	 * -- a set of nested OrderMarkers which express how the tuples of a nested
	 * child are ordered. There is no way of comparing tuples based on the
	 * values of their nested children ! This seems reasonable for now.
	 * 
	 * Simple value order is understood as follows: any two non-null values are
	 * in order; nulls come first. (unless we see this causes some problem...)
	 */
	protected OrderMarker orderMaker;

	protected PeerID peer;

	/**
	 * Number of tuples that were passed to the above operator.
	 */
	protected long numberOfTuples;

	protected static final int PRINTING_INDENTATION_TABS = 2;

	/**
	 * Constructs an NIterator with an arbitrary number of children.
	 * 
	 * @param children
	 *            the children
	 */
	public NIterator(List<NIterator> children) {
		// this.peer = new NormalNodeID();

		// creating an operator ID for this operator and reporting it

		numberOfTuples = 0;

		this.children = children;
	}

	/**
	 * Constructs a leaf NIterator by calling {@link NIterator#NIterator(List)}.
	 */
	public NIterator() {
		this(new ArrayList<NIterator>(0));
	}

	/**
	 * @return the orderMaker
	 */
	public OrderMarker getOrderMaker() {
		return orderMaker;
	}

	/**
	 * @param orderMaker
	 *            the orderMaker to set
	 */
	public void setOrderMaker(OrderMarker orderMaker) {
		this.orderMaker = orderMaker;
	}

	/**
	 * @return the peer
	 */
	public PeerID getPeer() {
		return peer;
	}

	/**
	 * Performs a DFS traversal of the tree under this operator and returns the
	 * list of operators under this operator.
	 * 
	 * @return a list of LogicalOperators
	 * 
	 * @author Asterios KATSIFODIMOS
	 * */
	public List<NIterator> getDescendants() {
		ArrayList<NIterator> operators = new ArrayList<NIterator>();
		this.getDescendantsRec(operators);
		return operators;
	}

	/**
	 * @author Asterios KATSIFODIMOS
	 * */
	private void getDescendantsRec(ArrayList<NIterator> descs) {
		if (children != null) {
			for (NIterator child : this.children) {
				child.getDescendantsRec(descs);
				descs.add(child);
			}
		}
	}

	/**
	 * Constructs a unary NIterator by calling {@link NIterator#NIterator(List)}
	 * .
	 * 
	 * @param child
	 *            the child
	 */
	public NIterator(NIterator child) {
		this(Arrays.asList(child));
	}

	/**
	 * Constructs a binary NIterator by calling
	 * {@link NIterator#NIterator(List)}.
	 * 
	 * @param left
	 *            the left child
	 * @param right
	 *            the right child
	 */
	public NIterator(NIterator left, NIterator right) {
		this(Arrays.asList(left, right));
	}

	/**
	 * Constructs an <i>n</i>-ary NIterator from an array of children by calling
	 * {@link NIterator#NIterator(List)}.
	 * 
	 * @param children
	 *            the array of children
	 */
	public NIterator(NIterator[] children) {
		this(Arrays.asList(children));
	}

	/**
	 * @return the children
	 */
	public List<NIterator> getChildren() {
		return children;
	}

	public void setPeer(PeerID peer) {
		this.peer = peer;
		assert (this.peer != null) : "Null peer";
	}

	public void setBindings(NTuple t) throws VIP2PExecutionException {
		for (NIterator child : children)
			child.setBindings(t);
	}

	/**
	 * to initialize the iterator
	 */
	public abstract void open() throws VIP2PExecutionException;

	/**
	 * Resets the iterator without having to re-open it
	 */
	public void initialize() throws VIP2PExecutionException {
		;
	}

	/**
	 * @return true if there is at least some top-level tuple inside
	 */
	public abstract boolean hasNext() throws VIP2PExecutionException;

	/**
	 * @return the next NTuple available
	 */
	public abstract NTuple next() throws VIP2PExecutionException;

	/**
	 * to close the iterator
	 */
	public abstract void close() throws VIP2PExecutionException;

	/**
	 * TODO: do we need this at the level of the interface ?
	 * 
	 * @param i
	 *            the colum at which there is a nested field
	 * @return an iterator for that field. The iterator has still to be opened
	 *         and closed.
	 */
	public abstract NIterator getNestedIterator(int i)
			throws VIP2PExecutionException;

	public abstract String getName();

	public abstract String getName(int depth);

	public void display() throws VIP2PExecutionException {
		long t1 = System.currentTimeMillis();
		int n = 0;
		// Parameters.logger.info(NodeInformation.localPID + ": \n"
		// + this.getName() + " " + this.getOrderMaker().toString());
		// Parameters.logger.info(NodeInformation.localPID + ": =============");
		this.open();
		assert (this.nrsmd != null) : "Null NRSMD";
		this.nrsmd.display();
		while (this.hasNext()) {
			NTuple nt = this.next();
			nt.display(this.getClass().getName());
			n++;
		}

		// Parameters.logger.info(NodeInformation.localPID + ": " + n
		// + " tuples ordered by " + this.getOrderMaker().toString()
		// + "\n");
		this.close();

		long t2 = System.currentTimeMillis();

		// Parameters.logger
		// .info(NodeInformation.localPID + ": Time:" + (t2 - t1));
	}

	public void display(StringBuffer sb) {
		long t1 = System.currentTimeMillis();
		try {
			int n = 0;
			sb.append("\n" + this.getName() + "\n");
			for (int i = 0; i < this.getName().length(); i++) {
				sb.append("=");
			}
			sb.append("\n");
			this.open();

			sb.append(this.nrsmd.toString());
			sb.append("\n");
			while (this.hasNext()) {
				NTuple nt = this.next();
				sb.append("   ");
				sb.append(nt.toString());
				sb.append("\n");
				n++;
			}

			sb.append(n + " tuples ordered by "
					+ this.getOrderMaker().toString() + "\n");
			this.close();
		} catch (Exception e) {
			// Parameters.logger.error("Exception: ", e);
		}

		long t2 = System.currentTimeMillis();

		// Parameters.logger.info(NodeInformation.localPID + ": times ="
		// + (t2 - t1));
		// Parameters.logger.info(NodeInformation.localPID + ": " + this.nrsmd);
	}

	public abstract NIterator copy() throws VIP2PExecutionException;

	public void bottomUpDisplay() throws VIP2PExecutionException {
		for (NIterator child : this.children) {
			child.bottomUpDisplay();
		}
		this.display();
	}

	public int sortCount() {
		int res = 0;
		if (this instanceof MemoryHashJoin) {
			MemoryHashJoin mhj = (MemoryHashJoin) this;
			res = mhj.left.sortCount() + mhj.right.sortCount();
		}
		return res;
	}

	/**
	 * method fills the String buffer with the code that represents the Physical
	 * Plans.
	 * 
	 * @return The identifier of the node that has been filled.
	 */
	public abstract int recursiveDotString(StringBuffer sb, int parentNo,
			int firstAvailableNo);

	/**
	 * Method that draws an image depicting the physical plan rooted by this
	 * physical operator.
	 */
	public void draw() {
		String fileName = createDot(createDotString());
		drawImageFromDot(fileName);
	}

	/**
	 * This method returns the content of the dot file that will give the image
	 * which represents the physical plan rooted by this operator.
	 * 
	 * @return
	 */
	public String createDotString() {
		StringBuffer sb = new StringBuffer();
		Calendar cal = new GregorianCalendar();
		sb.append("digraph  g{ graph[label = \""
				+ cal.get(Calendar.HOUR_OF_DAY) + ":"
				+ cal.get(Calendar.MINUTE) + ":" + cal.get(Calendar.SECOND)
				+ "." + cal.get(Calendar.MILLISECOND) + "\"]\n");
		sb.append("node [shape=rectangle, color=black, fontcolor=black, style=bold] ");
		sb.append("edge [color=black] ");
		recursiveDotString(sb, -1, 0);

		return sb.toString();
	}

	/**
	 * This method receives the contents of a dot file and it creates one with
	 * these data on the disk.
	 * 
	 * @param dotStr
	 *            The contents of the file.
	 * @return The filename of the .dot file omitting the termination.
	 */
	public static String createDot(String dotStr) {
		Calendar cal = new GregorianCalendar();
		String planFileName = "phys-" + cal.get(Calendar.HOUR_OF_DAY) + ":"
				+ cal.get(Calendar.MINUTE) + ":" + cal.get(Calendar.SECOND)
				+ ":" + cal.get(Calendar.MILLISECOND);
		try {
			String fileNameDot = new String(planFileName + ".dot");
			FileWriter file;
			file = new FileWriter(fileNameDot);
			file.write(dotStr);
			file.write("}\n");
			file.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return planFileName;
	}

	/**
	 * This method creates an image using a dot file.
	 * 
	 * @param givenFileName
	 *            The filename of the dot file without the termination.
	 */
	public static void drawImageFromDot(String givenFileName) {
		String fileNameDot = new String(givenFileName + ".dot");
		String fileNamePNG = new String(
				Parameters.getProperty("DOToutputFolder") + File.separator
						+ givenFileName + ".png");
		try {
			Runtime r = Runtime.getRuntime();
			// Parameters.logger.info(NodeInformation.localPID
			// + ": I am drawing monitoring image " + fileNamePNG);
			String com = new String("dot -Tpng " + fileNameDot + " -o "
					+ fileNamePNG);
			Process p;
			p = r.exec(com);

			p.waitFor();
			Process p2 = r.exec("rm " + fileNameDot + "\n");
			p2.waitFor();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	/**
	 * This method presents the pictorial representation of the physical Plan.
	 * 
	 * @param : The physical plan is drawn in the png file with the name
	 *        Filename....This method is very helpful in debugging as one can
	 *        identify the plan that has been drawn to this file.
	 */
	public void draw(String FileName) {
		StringBuffer sb = new StringBuffer();
		sb.append("digraph  g{\n");
		sb.append("node [shape=rectangle, color=blue2, fontcolor=blue4]");
		sb.append("edge [color=blue4]");
		recursiveDotString(sb, -1, 0);

		try {

			String pathName = "";
			int lastSlash = FileName.lastIndexOf(File.separator);

			if (lastSlash > 0) {
				pathName = FileName.substring(0, lastSlash);
				File dir = new File(pathName);
				dir.mkdirs();
			}

			int startIndex = 0;
			if (FileName.contains(File.separator)) {
				startIndex = FileName.lastIndexOf(File.separator) + 1;
			}
			int endIndex = FileName.length();
			if (FileName.contains(".")) {
				endIndex = FileName.lastIndexOf(".");
			}

			String fileNameDot = new String(FileName + ".dot");
			String fileNamePNG = new String(
					Parameters.getProperty("DOToutputFolder") + File.separator
							+ FileName.substring(startIndex, endIndex)
							+ "-PhyPlan.png");
			FileWriter file = new FileWriter(fileNameDot);

			file.write(new String(sb));
			file.write("}\n");
			file.close();

			Runtime r = Runtime.getRuntime();
			String com = new String("dot -Tpng " + fileNameDot + " -o "
					+ fileNamePNG);
			Process p = r.exec(com);
			p.waitFor();
			Process p2 = r.exec("rm " + fileNameDot + "\n");
			p2.waitFor();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * Returns the number of NetReceive Operations under this class.
	 * 
	 */
	public abstract int receiveCount();

	public void increaseOpNo() {
		totalOperatorNo++;
	}

	/**
	 * Performs a BFS traversal of the tree under this operator and returns the
	 * list of operators under this operator.
	 * 
	 * @return a list of LogicalOperators in a BFS manner
	 * 
	 * @author Karan AGGARWAL
	 * */

	public List<NIterator> getDescendantsBFS() {

		ArrayList<NIterator> nodes = new ArrayList<NIterator>();
		Queue<NIterator> BFSQueue = new LinkedList<NIterator>();
		BFSQueue.add(this);

		while (!BFSQueue.isEmpty()) {

			NIterator plan = BFSQueue.poll();
			nodes.add(plan);

			if (plan.children != null) {
				for (NIterator node : plan.children) {
					BFSQueue.add(node);
				}
			}

		}

		return nodes;
	}

	/**
	 * @return number of tabs as passed in the argument
	 * 
	 * @author Karan AGGARWAL
	 * */
	protected String getTabs(int space) {
		String spaces = "";
		for (int i = 0; i < space; i++) {
			spaces += "\t";
		}

		return spaces;
	}

}