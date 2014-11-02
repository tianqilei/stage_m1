package entityOfPostgresql;

public class LineItem {

	private String orderId;
	private String itemId;
	private int quantity;

	public LineItem(String orderId, String itemId, int quantity) {
		super();
		this.orderId = orderId;
		this.itemId = itemId;
		this.quantity = quantity;
	}

	public String getOrderId() {
		return orderId;
	}

	public void setOrderId(String orderId) {
		this.orderId = orderId;
	}

	public String getItemId() {
		return itemId;
	}

	public void setItemId(String itemId) {
		this.itemId = itemId;
	}

	public int getQuantity() {
		return quantity;
	}

	public void setQuantity(int quantity) {
		this.quantity = quantity;
	}

	@Override
	public String toString() {
		return "LineItem [orderId=" + orderId + ", itemId=" + itemId
				+ ", quantity=" + quantity + "]";
	}

	public String toOutputString(String seperator, String nexLine) {
		return getOrderId() + seperator + getItemId() + seperator
				+ getQuantity() + nexLine;
	}
}
