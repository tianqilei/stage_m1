package vip2p_copy;

import java.io.Serializable;
import java.util.ArrayList;


/**
 * This class can be used for more complex conjunctive predicates.
 * Its individual predicates can be of any kind.
 * 
 * @author Ioana MANOLESCU
 */
public class ConjunctivePredicate extends Predicate implements Serializable{

	private static final long serialVersionUID = 6242075439707963521L;
	
	public Predicate[] preds;
	
	public ConjunctivePredicate(Predicate pred, SimplePredicate sPred){
		ConjunctivePredicate conj = null;
		try{
			conj = (ConjunctivePredicate) pred;
			this.preds = new Predicate[conj.preds.length + 1];
			for (int i = 0; i < conj.preds.length; i ++){
				this.preds[i] = conj.preds[i];
			}
			this.preds[this.preds.length - 1] = sPred;
		}
		catch(ClassCastException e){
			try{
				SimplePredicate sp = (SimplePredicate) pred;
				this.preds = new Predicate[2];
				this.preds[0] = sp;
				this.preds[1] = sPred;
			}
			catch(ClassCastException e2){
			//	Parameters.logger.error("Wrong type");
			}
		}
		
	}
	
	public ConjunctivePredicate(Predicate[] preds){
		this.preds = preds;
	}
	
	/**
	 * Constructor that gets a list of SimplePredicates and returns the ConjunctivePredicate
	 * @author Konstantinos KARANASOS
	 */
	public ConjunctivePredicate(ArrayList<? extends Predicate> predsList) {
		this.preds = new Predicate[predsList.size()];
		
		for (int i = 0; i < predsList.size(); i++)
			this.preds[i] = predsList.get(i);
	}

	
	
	
	
	/* (non-Javadoc)
	 * @see fr.inria.gemo.uload.execution.Predicate#isTrue(fr.inria.gemo.uload.execution.NTuple)
	 */
	public boolean isTrue(NTuple t) throws VIP2PExecutionException {
		boolean res = true;
		for (int i = 0; i < preds.length; i ++){
			if (!preds[i].isTrue(t)){
				return false;
			}
		}
		return res;
	}
	
	/* (non-Javadoc)
	 * @see fr.inria.gemo.uload.execution.Predicate#toString()
	 */
	public String toString() {
		StringBuffer sb = new StringBuffer();
		sb.append("[");
		for (int i = 0; i < preds.length; i ++){
			sb.append(preds[i].toString());
			if (i < preds.length - 1){
				sb.append(",");
			}
		}
		sb.append("]");
		return new String(sb);
	}
	
	/**@author Karan AGGARWAL
	 *  
	 * @return String representation of Predicate separated by , 
	 * Follows the Plan file (.phyp) grammar
	 */
	public String getName() {
		StringBuffer sb = new StringBuffer();
		
		sb.append("[");
		for (int i = 0; i < preds.length; i ++){
			sb.append(preds[i].getName());
			if (i < preds.length - 1){
				sb.append(",");
			}
		}
		sb.append("]");
		
		return new String(sb);
	}
	
	/* (non-Javadoc)
	 * @see fr.inria.gemo.vip2p.predicates.Predicate#clone()
	 */
	@Override
	public Object clone() {
		Predicate clonedPreds[] = new Predicate[preds.length];
		for ( int i = 0; i<preds.length; i++ ) {
			clonedPreds[i] = (Predicate) preds[i].clone();
		}
		return new ConjunctivePredicate(clonedPreds);
	}
	
	
	/**
	 * 
	 * @return an array of simple predicates
	 */
	public Predicate[] getPreds(){
		return preds;
	}
}