package vip2p_copy;

 
import java.util.ArrayList;
import java.util.HashMap;

/**
 * The class is used to specify what to project out of nested tables.
 * If the table is not nested, it suffices to use an array of integers instead of a big ProjectMask.
 * Thus, so far, some functionality may exist only based on int arrays.
 * 
 * @author Ioana MANOLESCU
 * 
 * @created 23/08/2005
 */
@Deprecated
public class ProjMask {
	
	public ArrayList<Integer> columns;
	/**
	 * In this hash map: 
	 * for every column N which is kept, assume this is at position X in columns,
	 * we have a ProjMask which says what exactly to keep from column X.
	 * 
	 * Example:
	 * 
	 * to keep
	 *    column 0 (unnested)
	 *    column 1 (unnested)
	 *    column 1 again (unnested)
	 *    column 2 (nested), from which:
	 *                       keep column 3 (unnested)
	 *                       keep column 5 (unnested)
	 *    column 4 (unnested)
	 * 
	 * do:
	 * 
	 * -- the internal one;
	 * ProjMask m1 = new ProjMask();
	 * m1.addChild(3);
	 * m1.addChild(5);
	 * 
	 * -- the external one:
	 * 
	 * ProjMask m = new ProjMask();
	 * m.addChild(0); -- this will go at index 0
	 * m.addChild(1); -- this will go at index 1
	 * m.addChild(1); -- this will go at index 2
	 * m.addChild(2); -- this will go at index 3
	 * m.addChild(4); -- this will go at index 4
	 * m.addChildMask(3, m1) -- use 3 to refer to what is kept from column 2
	 * 
	 */
	public HashMap<Integer, ProjMask> keepFromChildren;
	
	//added by andrei to save the state of the projected fields at top levels 
	public int nextLevelAtTop=0;
	//end added by andrei
	
	public ProjMask(){
		this.columns = new ArrayList<Integer>();
		this.keepFromChildren = new HashMap<Integer, ProjMask>();
	}
	public ProjMask(ArrayList<Integer> columns){
		this.columns = columns;
		this.keepFromChildren = new HashMap<Integer, ProjMask>();
	}
	
	/**
	 * This says you want to keep column col, at top level
	 * @param col
	 */
	public void addChild(int col){
		this.columns.add(new Integer(col));
	}
	/**
	 * Say you want to associate forChild to the idx-th top-level column that you keep
	 * To learn which is the idx-th top level column, take columns.elementAt(idx)
	 * @param col
	 * @param forChild
	 */
	public void addChildMask(int idx, ProjMask forChild){
		this.keepFromChildren.put(new Integer(idx), forChild);
	//	Parameters.logger.debug("At " + idx + " put " + forChild.toString());
	}
	
	public void addChildMask(int[] col, ProjMask forChild){
		recAddChildMask(col, forChild, 0);
	}

	/**
	 * @param col
	 * @param i
	 */
	private void recAddChildMask(int[] col, ProjMask forChild, int from) {
		if (from == col.length - 1){
			addChildMask(col[from], forChild);
		}
		else{
			ProjMask child = (ProjMask)keepFromChildren.get(new Integer(col[from]));
			child.recAddChildMask(col, forChild, from + 1);
		}
	}
	
	public String toString(){
		StringBuffer sb = new StringBuffer();
		toString(sb);
		return new String(sb);
	}
	public void toString(StringBuffer sb){
		sb.append("[");
		for (int pos = 0; pos < columns.size(); pos ++){
			Integer i = (Integer)columns.get(pos);
			sb.append(i);
			ProjMask child = (ProjMask)keepFromChildren.get(new Integer(pos));
			if (child != null){
				sb.append(":");
				child.toString(sb);
			}
			sb.append(" ");
		}
		sb.append("]");
	}
	
}
