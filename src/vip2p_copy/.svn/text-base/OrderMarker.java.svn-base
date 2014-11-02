package vip2p_copy;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;

/**
 * It marks the order that the tuples should respect when they come out of a
 * physical operator.
 * 
 * @author Ioana MANOLESCU
 * 
 * @created 16/08/2005
 */
public class OrderMarker implements Serializable {

	private static final long serialVersionUID = 1375984598611255567L;

	// At position 0, we will find all columns which are guaranteed to be equal
	// and
	// both represent the dominant sort order.
	//
	// At position 1, we will find all columns which are guaranteed to be equal
	// and
	// both represent the second dominant sort order
	// etc.
	public int[][] order;

	OrderMarker[] children;
	int[] childIndices;
	// for every child, this says: for which of the operator's columns does this
	// order
	// apply.

	int[] iDirect;
	int iChild;

	public OrderMarker() {
		this.order = new int[0][];
		this.children = new OrderMarker[0];
		this.childIndices = new int[0];
	}

	/**
	 * k1 is the array containing the equivalence groups' sizes (at 0, the size
	 * of equivalence group 0, at 1, the size of equivalence group 1 etc.) k2 is
	 * the number of nested OrderMarkers
	 * 
	 * @param k1
	 * @param k2
	 */
	public OrderMarker(int[] k1, int k2) {
		// Parameters.logger.debug("Creating an OrderMarker of " + k1.length +
		// " columns  and "
		// + k2 + " nested OrderMarkers");
		this.order = new int[k1.length][];
		for (int i = 0; i < k1.length; i++) {
			this.order[i] = new int[k1[i]];
		}
		this.children = new OrderMarker[k2];
		this.childIndices = new int[k2];

		// iDirect seems to be the cursor in each equivalence group
		iDirect = new int[k1.length];
		for (int i = 0; i < k1.length; i++) {
			iDirect[i] = 0;
		}
		// Parameters.logger.debug("Allocated iDirect of " + iDirect.length +
		// " positions");

		iChild = 0;
	}

	public OrderMarker(int[][] newGroups, OrderMarker[] newChildren,
			int[] newIndices) {
		this.order = newGroups;
		this.children = newChildren;
		this.childIndices = newIndices;
	}

	public OrderMarker addToGroup(int iGroup, int iOther) {
		// Parameters.logger.debug("OrderMarker.addToGroup: add " + iOther +
		// " to the group of " + iGroup);
		int[][] o2 = new int[this.order.length][];
		for (int i = 0; i < order.length; i++) {
			boolean iGroupIsHere = false;
			for (int j = 0; j < this.order[i].length; j++) {
				if (this.order[i][j] == iGroup) {
					iGroupIsHere = true;
					break;
				}
			}
			if (iGroupIsHere) {
				// Parameters.logger.debug("Need to add " + iOther +
				// " in the group at position " + i);
				o2[i] = new int[order[i].length + 1];
				boolean otherPlaced = false;
				for (int j = 0, k = 0; j < o2[i].length; j++) {
					// Parameters.logger.debug("Figuring out new index at " +
					// j);
					boolean copyOther = false;
					if (k < order[i].length) {
						// Parameters.logger.debug("In group " + i + " at " + k
						// + " we have " + o[i][k]);
						if (order[i][k] > iOther) {
							copyOther = (!otherPlaced);
						}
					} else {
						copyOther = (!otherPlaced);
					}
					if (copyOther) {
						// Parameters.logger.debug("Other has not been placed! so placing it at position "
						// + j);
						o2[i][j] = iOther;
						otherPlaced = true;
					} else {
						// Parameters.logger.debug("Copying this!");
						o2[i][j] = order[i][k];
						k++;
					}
				}
			} else {
				o2[i] = order[i];
			}
		}
		return new OrderMarker(o2, this.children, this.childIndices);
	}

	public void addDirect(int n, int k) {
		int aux1 = iDirect[n];
		this.order[n][aux1] = k;
		iDirect[n]++;
	}

	public void addChild(OrderMarker c, int index) {
		this.children[iChild] = c;
		this.childIndices[iChild] = index;
		iChild++;
	}

	public boolean dominantOrder(int n) {
		if (this.order.length == 0) {
			return false;
		}
		for (int i = 0; i < this.order[0].length; i++) {
			if (order[0][i] == n) {
				return true;
			}
		}
		return false;
	}

	/**
	 * Constructs an order marker by appending the second after the first. This
	 * is appropriate when doing e.g. nested loops join, when the order of the
	 * results follows, first, tuples from the first input, and then, tuples
	 * from the second one.
	 * 
	 * @param m1
	 * @param m2
	 * @return
	 */
	public static OrderMarker append(OrderMarker m1, OrderMarker m2) {
		int[] lengths = new int[m1.order.length + m2.order.length];
		for (int i = 0; i < lengths.length; i++) {
			if (i < m1.order.length) {
				lengths[i] = m1.order[i].length;
			} else {
				lengths[i] = m2.order[i - m1.order.length].length;
			}
		}
		OrderMarker res = new OrderMarker(lengths, m1.children.length
				+ m2.children.length);
		for (int i = 0; i < m1.order.length; i++) {
			for (int j = 0; j < m1.order[i].length; j++) {
				res.addDirect(i, m1.order[i][j]);
			}
		}
		for (int i = 0; i < m2.order.length; i++) {
			for (int j = 0; j < m2.order[i].length; j++) {
				res.addDirect(i + m1.order.length, m2.order[i][j]);
			}
		}
		for (int i = 0; i < m1.children.length; i++) {
			res.addChild(m1.children[i], m1.childIndices[i]);
		}
		for (int i = 0; i < m2.children.length; i++) {
			res.addChild(m2.children[i], m2.childIndices[i]);
		}
		return res;
	}

	/**
	 * Computes the order marker that results from a nested loops join on an
	 * equality predicate. Tuples are sorted first, by the first groups as they
	 * were, then by the second groups, but the group containing n1 is merged
	 * with the group containing n2.
	 * 
	 * @param m1
	 * @param m2
	 * @param n1
	 * @param n2
	 * @return
	 */
	public static OrderMarker nlEquiJoin(OrderMarker m1, OrderMarker m2,
			int n1, int n2, int shift) {
		if (m1.order.length == 0) {
			// if the first operator was not ordered, then there is going to be
			// no order
			// on the result
			return new OrderMarker();
		}

		// Parameters.logger.debug("M1 is: " + m1.toString() + " joining on " +
		// n1);
		// Parameters.logger.debug("M2 is: " + m2.toString() + " joining on " +
		// n2);
		OrderMarker aux1 = OrderMarker.shift(m2, shift);
		OrderMarker aux = OrderMarker.append(m1, aux1);
		// Parameters.logger.debug("Aux is: " + aux.toString());

		int groupOfN1 = -1;
		for (int i = 0; i < m1.order.length; i++) {
			for (int k = 0; k < m1.order[i].length; k++) {
				if (m1.order[i][k] == n1) {
					groupOfN1 = i;
				}
			}
		}
		int groupOfN2 = -1;
		for (int i = 0; i < m2.order.length; i++) {
			for (int k = 0; k < m2.order[i].length; k++) {
				if (m2.order[i][k] == n2) {
					groupOfN2 = i;
				}
			}
		}
		if (groupOfN1 < 0 || groupOfN2 < 0) {
			// one or both of the join columns did not participate in the order.
			return aux;
		}
		// both indices participated in some order groups, which now make 2.
		// we need to collapse the second over the first

		// the new group of N2 in aux
		int newGroupOfN2 = groupOfN2 + m1.order.length;

		// Parameters.logger.debug("Group of n1(" + n1 + "): " + groupOfN1);
		// Parameters.logger.debug("Group of n2(" + n2 + "): " + groupOfN2 +
		// " new group of n2: " + newGroupOfN2);

		int k = aux.order.length - 1;
		int[] newSizes = new int[k];
		// Parameters.logger.debug("There are " + k + "  new groups from 0 to "
		// + (k-1));
		for (int i = 0; i < k; i++) {
			if (i == groupOfN1) {
				// Parameters.logger.debug("Adding in group " + groupOfN1 +
				// " previous groups " + groupOfN1 + " and " + groupOfN2);
				newSizes[i] = m1.order[groupOfN1].length
						+ m2.order[groupOfN2].length;
				// Parameters.logger.debug("Res newSizes at " + i + " " +
				// newSizes[i]);
			} else {
				if (i < newGroupOfN2) {
					newSizes[i] = aux.order[i].length;
					// Parameters.logger.debug("Res newSizes at " + i + " " +
					// newSizes[i]);
				} else {
					if (i >= newGroupOfN2) {
						newSizes[i] = aux.order[i + 1].length;
						// Parameters.logger.debug("Res newSizes at " + i + " "
						// + newSizes[i]);
					}
				}
			}
		}

		OrderMarker res = new OrderMarker(newSizes, aux.childIndices.length);
		for (int i = 0; i < aux.childIndices.length; i++) {
			res.addChild(aux.children[i], aux.childIndices[i]);
		}

		// Parameters.logger.debug("Res is: " + res.toString());
		for (int i = 0; i < res.order.length; i++) {
			// Parameters.logger.debug("FILLING IN RES AT " + i);
			// Parameters.logger.debug("Res[" + i + "] has length " +
			// res.o[i].length);
			if (i == groupOfN1) {
				for (int j = 0; j < m1.order[groupOfN1].length; j++) {
					// Parameters.logger.debug("Aux.o has length " +
					// aux.o.length);
					// Parameters.logger.debug("Aux[" + groupOfN1 +
					// "] has length " + m1.o[groupOfN1].length);
					// Parameters.logger.debug("J is " + j);
					res.addDirect(groupOfN1, aux.order[groupOfN1][j]);
				}
				for (int j = 0; j < m2.order[groupOfN2].length; j++) {
					res.addDirect(groupOfN1, aux.order[newGroupOfN2][j]);
				}
			} else {
				if (i < newGroupOfN2) {
					// Parameters.logger.debug("Copying " + i + " from " + i +
					// " in Aux");
					for (int j = 0; j < res.order[i].length; j++) {
						// Parameters.logger.debug("Copying in res at " + i +
						// " " + aux.o[i][j]);
						res.addDirect(i, aux.order[i][j]);
					}
				} else {
					// here i >= newGroupOfN2
					// Parameters.logger.debug("Copying " + i + " from " + (i+1)
					// + " in Aux");
					for (int j = 0; j < res.order[i].length; j++) {
						// Parameters.logger.debug("Copying in res at " + i +
						// " " + aux.o[i+1][j]);
						res.addDirect(i, aux.order[i + 1][j]);
					}
				}
			}
		}
		for (int i = 0; i < aux.childIndices.length; i++) {
			res.childIndices[i] = aux.childIndices[i];
			res.children[i] = aux.children[i];
		}

		return res;
	}

	/**
	 * Constructs an order marker obtained by projecting only the keepColumns
	 * from m1.
	 * 
	 * @param m1
	 * @param keepColumns
	 * @return
	 */
	public static OrderMarker project(OrderMarker m1, int[] keepColumns) {
		int directKeep = 0;
		int[] directSizes;
		int[] directIndices;
		int childKeep = 0;

		// true as long as we keep at least some of the columns originally in
		// this group.
		boolean masterOrderKept = true;

		// learn how many groups we will keep, and of which sizes

		ArrayList<Integer> directKeepGroups = new ArrayList<Integer>();
		ArrayList<Integer> directKeepSizes = new ArrayList<Integer>();
		// for every original group, see if we keep it
		// we only go up to the first group that we do not preserve
		for (int i = 0; i < m1.order.length && masterOrderKept; i++) {
			// how many indices from this group we keep
			int keepFromOrderGroupI = 0;
			// for every colum that is kept
			for (int j = 0; j < keepColumns.length; j++) {
				// if that column was somewhere part of this group
				for (int k = 0; k < m1.order[i].length; k++) {
					if (m1.order[i][k] == keepColumns[j]) {
						// then there will be an extra column kept from this
						// group
						keepFromOrderGroupI++;
					}
				}
			}
			// if something is kept from this group
			if (keepFromOrderGroupI > 0) {
				// Parameters.logger.debug("Keeping some of group " + i);
				// then mark which group that is
				directKeepGroups.add(new Integer(i));
				// and how many indices from the group are kept
				directKeepSizes.add(new Integer(keepFromOrderGroupI));
			} else {// This group not kept.
				masterOrderKept = false;
			}
		}

		// Parameters.logger.debug("The projection preserves: " +
		// directKeepGroups.size()+ " order marker groups");

		directKeep = directKeepGroups.size();

		if (directKeepGroups.size() == 0) {
			directSizes = new int[directKeep];
			directIndices = new int[directKeep];
		} else {
			// move the indices and the sizes into arrays
			directSizes = new int[directKeep];
			directIndices = new int[directKeep];
			Iterator<Integer> ig = directKeepGroups.iterator();
			Iterator<Integer> is = directKeepSizes.iterator();
			int iAux = 0;
			while (ig.hasNext()) {
				directIndices[iAux] = ig.next().intValue();
				directSizes[iAux] = is.next().intValue();
				iAux++;
			}
		}

		// fill in the children order markers:

		// count how many children (markers) we keep
		// Parameters.logger.debug("There were " + m1.children.length +
		// " nested child orders in m1");
		for (int i = 0; i < m1.children.length; i++) {
			for (int j = 0; j < keepColumns.length; j++) {
				if (m1.childIndices[i] == keepColumns[j]) {
					// Parameters.logger.debug("The " + i +
					// "-th child order from m1 occurs at  "+
					// m1.childIndices[i] + " which is also the " + j +
					// "-th kept column");
					childKeep++;
				} else {
					// Parameters.logger.debug("The " + i +
					// "-th child order from m1 occurs at " +
					// m1.childIndices[i] + " which is different from the " + j
					// +
					// "-th kept column, namely " + keepColumns[j]);
				}
			}
		}
		// transcribe the direct groups
		OrderMarker om = new OrderMarker(directSizes, childKeep);
		// for every group of the previous marker
		for (int j = 0; j < directKeep; j++) {
			// Parameters.logger.debug("Looking at what to keep from group " + j
			// + " of the previous marker");
			// for every column that is kept
			for (int i = 0; i < keepColumns.length; i++) {
				// for every index in the group
				for (int k = 0; k < m1.order[j].length; k++) {
					// if the kept columns occurs in this group
					if (keepColumns[i] == m1.order[j][k]) {
						// Parameters.logger.debug("The " + i +
						// "-th column to keep, that is " + keepColumns[i] +
						// ", comes from the original group number " + j +
						// " and was at position " + k);
						// then add i (the new index of the column keep[i])
						// to the group, of which we have to find the index in
						// the new groups
						om.addDirect(locateInt(directIndices, j), i);
					}
				}
			}
		}
		// transcribe the children
		for (int j = 0; j < m1.children.length; j++) {
			// Parameters.logger.debug("Seeking to know if the order marker at child index "
			// + j + " will survive");
			for (int i = 0; i < keepColumns.length; i++) {
				if (keepColumns[i] == m1.childIndices[j]) {
					// Parameters.logger.debug("Kept column " + keepColumns[i] +
					// " matches child index in m1 at " + j);
					om.addChild(m1.children[j], i);

					// Parameters.logger.debug("Copied " + j +
					// "-th child order marker, namely ");
					// Parameters.logger.debug(om.children[j].toString() +
					// " as child marker at " + i + " in result");
				} else {
					// Parameters.logger.debug("Kept column " + keepColumns[i] +
					// " does not match child index in m1 at " + j);
				}
			}
		}
		return om;
	}

	/**
	 * This method computes the order marker of a Project operator, such that
	 * the projection is specified by a projection mask (typically used for
	 * nested tuples).
	 * 
	 * @param m1
	 * @param pm
	 * @return
	 */
	public static OrderMarker project(OrderMarker m1, ProjMask pm) {
		// Parameters.logger.debug("\nDEEP PROJECTING " + m1 + " BY " + pm);
		int[] cols = new int[pm.columns.size()];
		for (int i = 0; i < cols.length; i++) {
			cols[i] = ((Integer) pm.columns.get(i)).intValue();
		}

		// intermediary result
		OrderMarker res = project(m1, cols);
		// Parameters.logger.debug("Intermediary result: ");
		// Parameters.logger.debug(res.toString());

		Iterator<Integer> it = pm.keepFromChildren.keySet().iterator();
		while (it.hasNext()) {
			Integer Kept = (Integer) it.next();
			int keptIdx = Kept.intValue();
			ProjMask maskChild = (ProjMask) pm.keepFromChildren.get(Kept);
			// Parameters.logger.debug("Kept child at " + keptIdx +
			// " should be cut by: " + maskChild.toString());

			OrderMarker orderChild = res.locate(keptIdx);
			// Parameters.logger.debug("Intermediary order at " + keptIdx +
			// " is: " + orderChild.toString());

			OrderMarker cutChild = project(orderChild, maskChild);
			// Parameters.logger.debug("Cut child: " + cutChild.toString());
		}

		return res;
	}

	private static int locateInt(int[] v, int n) {
		for (int i = 0; i < v.length; i++) {
			if (v[i] == n) {
				return i;
			}
		}
		// Parameters.logger.error(n + " not found in array");
		return -1;
	}

	/**
	 * Constructs an order marker obtained by copying om but m columns to the
	 * right. This is appropriate e.g. when doing a StackTreeDescendant join,
	 * which preserves the descendant order, however, the descendant part of the
	 * tuple is "translated" to the right by the size of the ancestor tuples.
	 * 
	 * @param om
	 * @param k
	 * @return
	 */
	public static OrderMarker shift(OrderMarker om, int k) {
		int[] dSizes = new int[om.order.length];
		for (int i = 0; i < dSizes.length; i++) {
			dSizes[i] = om.order[i].length;
		}
		OrderMarker res = new OrderMarker(dSizes, om.children.length);
		for (int i = 0; i < om.order.length; i++) {
			for (int j = 0; j < om.order[i].length; j++) {
				res.addDirect(i, (om.order[i][j] + k));
			}
		}
		for (int i = 0; i < om.children.length; i++) {
			res.addChild(om.children[i], i + k);
		}
		return res;
	}

	public void toString(StringBuffer sb) {
		// inside square brackets, print one after another all the equivalence
		// ordering
		// groups, in the form: [0=2=3,1=4]
		sb.append("[");
		for (int i = 0; i < order.length; i++) {
			for (int k = 0; k < order[i].length; k++) {
				sb.append(order[i][k]);
				if (k < order[i].length - 1) {
					sb.append("=");
				}
			}
			if (i < order.length - 1) {
				sb.append(",");
			}
		}
		sb.append("]{");
		// now start printing the children's orderings
		for (int i = 0; i < children.length; i++) {
			// the child at index i
			sb.append(childIndices[i] + ": ");
			if (children[i] == null) {
				sb.append("null");
			} else {
				// is ordered this way
				children[i].toString(sb);
			}
			if (i < children.length - 1) {
				sb.append(",");
			}
		}
		sb.append("}");
	}

	public String toString() {
		StringBuffer sb = new StringBuffer();
		toString(sb);
		return new String(sb);
	}

	/**
	 * @param n
	 *            : a colum index (overall)
	 * @return i: the children OrderMarker such that childrendIndices[i] = n
	 */
	public OrderMarker locate(int n) {
		for (int i = 0; i < children.length; i++) {
			if (childIndices[i] == n) {
				return children[i];
			}
		}
		return new OrderMarker();
	}

	/**
	 * The question asked here is: is it true that the last position in this int
	 * array is a dominant order in the order marker of which it is a direct
	 * component ?
	 * 
	 * Example: dominantOrder([1,2,1]) asks if 1 is a dominant order in the
	 * child order found at index 2 in the child order found at index 1 in this
	 * OrderMarker
	 * 
	 * Example: dominantOrder([1]) is the same as dominantOrder(1)
	 * 
	 * @param ancPath
	 * @return
	 */
	public boolean dominantOrder(int[] ancPath) {
		return recDominantOrder(ancPath, 0);
	}

	public boolean recDominantOrder(int[] ancPath, int from) {
		if (from == ancPath.length - 1) {
			return dominantOrder(ancPath[from]);
		} else {
			OrderMarker child = locate(ancPath[from]);
			return child.recDominantOrder(ancPath, from + 1);
		}
	}

	/**
	 * @param marker
	 * @param ancPath
	 * @param sortColumns
	 * @return
	 */
	public static OrderMarker sort(OrderMarker marker, int[] ancPath,
			int[] sortColumns) {
		return sort(marker, ancPath, sortColumns, 0);
	}

	/**
	 * @param marker
	 * @param ancPath
	 * @param sortColumns
	 * @param i
	 * @return
	 */
	private static OrderMarker sort(OrderMarker marker, int[] ancPath,
			int[] sortColumns, int from) {
		// Parameters.logger.debug("\nCOMPLEX SORT " + marker + " " +
		// marker.hashCode() + " FROM " + from);
		if (from == ancPath.length) {
			OrderMarker res = sort(marker, sortColumns);
			// Parameters.logger.debug("COMPLEX SORT OBTAINED " + res);
			return res;
		} else {
			OrderMarker res = null;
			int currentCol = ancPath[from];
			// this is a nested columns into which we have to sort some more.
			OrderMarker orderOfCurrentCol = marker.locate(currentCol);
			// Parameters.logger.debug("OrderMarker at child " + ancPath[from] +
			// " " + orderOfCurrentCol);
			OrderMarker replacedOrderOfCurrentCol = sort(orderOfCurrentCol,
					ancPath, sortColumns, from + 1);
			// Parameters.logger.debug("Replacement at child " + ancPath[from] +
			// " " + replacedOrderOfCurrentCol);

			boolean childWasOrdered = false;
			for (int i = 0; i < marker.childIndices.length; i++) {
				if (marker.childIndices[i] == ancPath[from]) {
					childWasOrdered = true;
				}
			}
			if (!childWasOrdered) {
				// child was not ordered, so we are now creating a new group.
				// We are going to put this group between the group of a lesser
				// index,
				// and the group of a larger index.
				// Anyway it doesn't really matter; the order among sibling
				// nested children
				// is immaterial.
				int[] newIndices = new int[marker.children.length + 1];
				OrderMarker[] newChildren = new OrderMarker[marker.children.length + 1];
				int n = 0;
				for (int i = 0; i < marker.childIndices.length; i++) {
					if (marker.childIndices[i] < ancPath[from]) {
						newChildren[n] = marker.children[i];
						newIndices[n] = marker.childIndices[i];
						n++;
					}
				}
				newChildren[n] = replacedOrderOfCurrentCol;
				newIndices[n] = ancPath[from];
				for (int i = 0; i < marker.childIndices.length; i++) {
					if (marker.childIndices[i] > ancPath[from]) {
						newChildren[n] = marker.children[i];
						newIndices[n] = marker.childIndices[i];
						n++;
					}
				}
				res = new OrderMarker(marker.order, newChildren, newIndices);
			} else { // child was ordered already, so just replace the
						// respective order spec
				marker.children[locateInt(marker.childIndices, ancPath[from])] = replacedOrderOfCurrentCol;
				res = marker;
			}

			// Parameters.logger.debug("Replacing inside " + orderOfCurrentCol +
			// " with " + replacedOrderOfCurrentCol);
			// Parameters.logger.debug("COMPLEX SORT CONSTRUCTED " + res);
			return res;

		}
	}

	/**
	 * @param marker
	 * @param sortColumns
	 * @return
	 */
	public static OrderMarker sort(OrderMarker marker, int[] sortColumns) {

		// at every i, gives the new index of this group. If the group has
		// disappeared, it is -1
		int[] groupMaps = new int[marker.order.length];
		for (int i = 0; i < groupMaps.length; i++) {
			groupMaps[i] = -1;
		}

		boolean[] alreadyInAGroup = new boolean[sortColumns.length];

		int remainingGroups = 0;

		for (int i = 0; i < sortColumns.length; i++) {
			alreadyInAGroup[i] = false;

			int thisCol = sortColumns[i];
			for (int j = 0; j < marker.order.length; j++) {
				for (int k = 0; k < marker.order[j].length; k++) {
					if (marker.order[j][k] == thisCol) {
						groupMaps[j] = i;
						// Parameters.logger.debug("The " + j +
						// "-th order group becomes now " + i + "-th");
						remainingGroups++;
						alreadyInAGroup[i] = true;
					}
				}
			}
			if (!alreadyInAGroup[i]) {
				// Parameters.logger.debug("Will create new group for " +
				// sortColumns[i]);
				remainingGroups++;
			}
		}

		int[] newDirectSizes = new int[remainingGroups];
		int currentGroup = 0;
		for (int i = 0; i < sortColumns.length; i++) {
			if (!alreadyInAGroup[i]) {
				newDirectSizes[currentGroup] = 1;
				// Parameters.logger.debug("Group number " + currentGroup +
				// " has " + currentGroup +
				// " columns");
				currentGroup++;
			} else {
				for (int j = 0; j < groupMaps.length; j++) {
					if (groupMaps[j] > -1) {
						if (groupMaps[j] == currentGroup) {
							newDirectSizes[currentGroup] = marker.order[j].length;
							// Parameters.logger.debug("Group number " +
							// currentGroup + " has " + currentGroup +
							// " columns");
							currentGroup++;
						}
					}
				}
			}
		}

		OrderMarker res = new OrderMarker(newDirectSizes,
				marker.children.length);

		for (int i = 0; i < sortColumns.length; i++) {
			if (!alreadyInAGroup[i]) {
				res.order[i][0] = sortColumns[i];
			} else {
				// copy group i in res from the group in o.marker at j such that
				// groupMaps[j]=i
				for (int j = 0; j < marker.order.length; j++) {
		//			Parameters.logger.debug("marker.o.length: "
			//				+ marker.order.length);
		//			Parameters.logger.debug("groupMaps.length: "
				//			+ groupMaps.length);
		//			Parameters.logger.debug("j: " + j);
					try {
						if (groupMaps[j] == i) {
							for (int k = 0; k < marker.order[j].length; k++) {
								res.order[i][k] = marker.order[j][k];
							}
						}
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			}
		}

		for (int i = 0; i < marker.children.length; i++) {
			res.addChild(marker.children[i], marker.childIndices[i]);
		}
		// Parameters.logger.debug("Obtained " + res);
		return res;
	}

}
