package vip2p_copy;

import java.io.Serializable;

/**
 * Class that represents a document ID.
 * 
 * @author Ioana MANOLESCU
 *
 */
public class DocNtwID implements Serializable{

	private static final long serialVersionUID = 1L;
	
	public PeerID nodeId;
	public String docName;
	
	public static int docNameCounter = 0;
	
	public DocNtwID(PeerID givNodeId, String givenDocName)
	{
		this.nodeId = givNodeId;
		this.docName = givenDocName;
	}
	
	public String toString()
	{
		return new String(this.nodeId + ":" + this.docName);
	}
	
	public boolean equals(Object givenObj)
	{
		if(!(givenObj instanceof DocNtwID))
			return false;
		
		DocNtwID givenPID = (DocNtwID) givenObj;
		if(!this.nodeId.equals(givenPID.nodeId))
			return false;
		if(!this.docName.equals(this.docName))
			return false;
		
		return true;
	}
	
	public int hashCode()
	{
		Long hc = new Long(this.nodeId.hashCode());
		hc += this.docName.hashCode();
		hc = hc % Integer.MAX_VALUE;
		
		return hc.intValue();
	}
}
