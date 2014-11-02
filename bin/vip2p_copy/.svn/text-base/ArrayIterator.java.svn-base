package vip2p_copy;

import java.util.ArrayList;
import java.util.Iterator;

/**
 * 
 * @author Ioana MANOLESCU
 * 
 * @created 28/07/2005
 * 
 */
public class ArrayIterator extends NIterator {

	private static final long serialVersionUID = -8416301341963231262L;

	Iterator<NTuple> i;

	NTuple t;
	ArrayList<NTuple> v;

	public ArrayIterator(ArrayList<NTuple> v, NRSMD newNRSMD) {
		this(v, newNRSMD, "");
		increaseOpNo();
	}

	public ArrayIterator(ArrayList<NTuple> arrayList, NRSMD newNRSMD,
			String xamName) {
		this.v = arrayList;
		this.nrsmd = newNRSMD;

	//	Parameters.logger.info("ArrayIterator nrsmd: " + this.nrsmd);
	//	Parameters.logger.info("ArrayIterator tuples: " + arrayList);

		for (int i = 0; i < arrayList.size(); i++) {
			NTuple t = arrayList.get(i);
			try {
				t.display();
			} catch (Exception e) {
				// TODO: handle exception
			}
		}
		assert (this.nrsmd != null) : "Null NRSMD";
		this.setOrderMaker(new OrderMarker());
		increaseOpNo();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see fr.inria.gemo.uload.execution.NIterator#open()
	 */
	public void open() throws VIP2PExecutionException {
		i = v.iterator();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see fr.inria.gemo.uload.execution.NIterator#hasNext()
	 */
	public boolean hasNext() throws VIP2PExecutionException {
		return i.hasNext();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see fr.inria.gemo.uload.execution.NIterator#next()
	 */
	public NTuple next() throws VIP2PExecutionException {
		this.t = (NTuple) i.next();
		this.numberOfTuples++;
		return t;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see fr.inria.gemo.uload.execution.NIterator#close()
	 */
	public void close() throws VIP2PExecutionException {
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see fr.inria.gemo.uload.execution.NIterator#getNestedIterator(int)
	 */
	public NIterator getNestedIterator(int i) throws VIP2PExecutionException {
		if (t.nestedFields == null) {
			t.display();
			throw new VIP2PExecutionException("No nested fields ");
		}
		if (i >= t.nestedFields.length) {
			t.display();
			throw new VIP2PExecutionException("Nested field index " + i
					+ " larger than " + (t.nestedFields.length - 1));
		}
		ArrayList<NTuple> aux = t.nestedFields[i];
		return new ArrayIterator(aux, t.nrsmd.getNestedChild(i), "");

	}

	@Override
	public String getName(int depth) {

		return null;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see fr.inria.gemo.uload.execution.NIterator#copy()
	 */
	public NIterator copy() throws VIP2PExecutionException {
		return new ArrayIterator(this.v, this.nrsmd);
	}

	/**
	 * Adds the code for the graphical representation to the StringBuffer.
	 * 
	 * @author Aditya SOMANI
	 */
	public final int recursiveDotString(StringBuffer sb, int parentNo,
			int firstAvailableNo) {
		if (parentNo != -1) {
			sb.append(parentNo + " -> " + firstAvailableNo + " ; \n");
		}

		return (firstAvailableNo + 1);
	}

	/**
	 * Returns the number of NetReceive Operations under this class.
	 * 
	 * @author Aditya SOMANI
	 */
	public int receiveCount() {
		return 0;
	}

	@Override
	public String getName() {
		// TODO Auto-generated method stub
		return null;
	}

}
