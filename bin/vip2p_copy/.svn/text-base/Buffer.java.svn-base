package vip2p_copy;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * @author Asterios KATSIFODIMOS
 *
 * Buffer with a thread that reads up to a given number of tuples
 * using a thread and keeps them in a blocking queue. When a new tuple arrives
 * the buffer "notifies" hasNext that a new tuple has arrived.
 * 
 * This buffer also keeps the previous tuple that was returned. When needed, the operator can 
 * rollback the previous tuple and serve that one instead of a new one.
 *   
 */
public class Buffer extends NIterator {

	private static final long serialVersionUID = 6774908651966365330L;

	private static final int BUFFER_SIZE = 1000;
	
	private NIterator child = null;
	
	//the thread that reads tuples from its child in a non-stop fashion
	private TuplesReader producer = null;
	
	//signal to the producer that the operator is going to close
	private boolean closeRequested = false;

	//Keeps the tuples of the buffer
	private BlockingQueue<NTuple> buffer = new LinkedBlockingQueue<NTuple>(BUFFER_SIZE);
	
	//A single rollback instructs this buffer to return to the previous tuple returned.		
	//This field is used to keep that previous tuple
	private NTuple previousTuple = null;
	
	//Holds the tuple that is giong to be returned on any call of next().
	private NTuple currentTuple = null;

	/**
	 * @param child, the child of this operator. 
	 */
	public Buffer(NIterator child) {
		super(child);
		this.child  = child;
		this.setOrderMaker(child.getOrderMaker());
		this.nrsmd = child.nrsmd;
		this.setPeer(child.getPeer());
		this.closeRequested = false;
		Thread.currentThread().setName("Buffer (consumer)");
	}
	
	class TuplesReader extends Thread {
		Buffer bufferThread;
		
		public TuplesReader(Buffer buf){
			this.bufferThread = buf;
		}
		
		public void run() {
			try{
				//runs until close is requested
				while(!closeRequested){					
					//if there's a next tuple in the input
					//just consume it and add it in the buffer
					if(child.hasNext()){
						try {
							buffer.put(child.next());
							//Parameters.logger.info(Thread.currentThread().getId() + " | Producer: " + "I just added a tuple to the buffer");
						} catch (InterruptedException e) {}
					}else{
						//if there's no more tuples left in this input 
						//we are going to add a marker in the
						//buffer (a null tuple) that the document
						//that we are currently processing has ended
						//this will be then used by the hasNext() operation
						try {
							buffer.put(NTuple.nullTuple(null));
						} catch (InterruptedException e) {}
					}
				}
			}catch(VIP2PExecutionException e){
				e.printStackTrace();
			//	Parameters.logger.error(e.toString());
			}
		}
	}
	
	@Override
	public void open() throws VIP2PExecutionException {
		this.child.open();
		producer = new TuplesReader(this);
		producer.setName("Buffer (producer)");
		producer.start();
	}

	@Override
	public boolean hasNext() throws VIP2PExecutionException {
		
		//Parameters.logger.info(Thread.currentThread().getId() + " | I just asked if there's a next!");
		
		//if a tuple is already "loaded", return true
		if(currentTuple != null ){
			return true;
		}
		
		//Take a tuple from the actual buffer.	
		try {
			//Wait until something enters the buffer. If something does not come in 50ms, try to see if
			//the producer is still alive (if not, it means that the operator is closing).
			while((currentTuple = buffer.poll(50, TimeUnit.MILLISECONDS))==null){
				
				//if the buffer is empty and the producer is dead, 
				//return false
				if((buffer.isEmpty() && !producer.isAlive()) || closeRequested){
					currentTuple = null;
					return false;
				}
				
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
		}			
	
		//if we see the case where we have an "end-of-document" we will 
		//have to inform the parent of this operator of it (with false in hasNext)
		if(currentTuple.isNull()){
			//Parameters.logger.info(Thread.currentThread().getId() + " | Consumer: " + "I just received a NULL tuple. Returning false.");
			currentTuple = null;
			return false;
		}

		//Parameters.logger.info(Thread.currentThread().getId() + " | Consumer: " + "I just received a tuple.");
		return true;
	}

	public void rollBack(){
		currentTuple = previousTuple;
	}
	
	@Override
	public NTuple next() throws VIP2PExecutionException {		
		previousTuple = currentTuple;
		currentTuple = null;
		return previousTuple;
	}

	@Override
	public void close() throws VIP2PExecutionException {
		this.closeRequested = true;
		this.child.close();
		this.buffer.clear();
		this.currentTuple = null;
	}

	@Override
	public NIterator getNestedIterator(int i) throws VIP2PExecutionException {
		throw new VIP2PExecutionException("getNestedIterator is not implemented for Buffer.java");
	}

	@Override
	public String getName() {
		return this.child.getName(); 
	}


	@Override
	public String getName(int depth) {
		String spaceForIndent = getTabs(PRINTING_INDENTATION_TABS*depth);
		return "\n" + spaceForIndent + "Buffer(" + this.child.getName(1 + depth) + "\n" + spaceForIndent + ")"; 
	}
	
	@Override
	public NIterator copy() throws VIP2PExecutionException {
		throw new VIP2PExecutionException("getNestedIterator is not implemented for Buffer.java");
	}

	@Override
	public int recursiveDotString(StringBuffer sb, int parentNo, int firstAvailableNo) {
		return this.child.recursiveDotString(sb,parentNo,firstAvailableNo);
	}

	@Override
	public int receiveCount() {
		return  this.child.receiveCount();
	}

}
