package vip2p_copy;

/**
 * Information about how and where is a given data collection reachable
 * One of the fields ends up being the XAM itself... (as a string) :-(
 * It would be better to kill the storedXAM pair class from StorageConfiguration,
 * and simply nest the XAM string (or the XAM itself, for that matter) in the storage
 * reference implementations.
 * And then, no need to parse the XAM multiple times later.
 * 
 * @author Ioana MANOLESCU
 */
public interface StorageReference {
	
	/**
	 * @return number of properties characterizing this reference
	 */
	public int getPropertiesNumber();
	
	/**
	 * @param i
	 * @return i-th property name
	 */
	public String getPropertyName(int i);
	

	/**
	 * @param i
	 * @return i-th property value
	 */
	public String getPropertyValue(int i);
	
	/**
	 * @param propertyName
	 * @return the value for this property
	 */
	public String getPropertyValue(String propertyName);
	
	/**
	 * Sets the property to a given value
	 * @param propertyName the name
	 * @param propertyValue the value
	 */
	public void setProperty(String propertyName, String propertyValue) throws Exception;
	
	/**
	 * Every storage reference refers to data stored on a given peer.
	 * @return
	 */
	public PeerID getPeerID();

}