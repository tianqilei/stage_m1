package entityOfPostgresql;

public class Ordered {

	private String idOrder;
	private String idUsr;
	private String idShop;
	private int year;
	private int month;
	private int day;
	private float totalPrice;

	public Ordered(String idOrder, String idUsr, String idShop, int year,
			int month, int day, float totalPrice) {
		super();
		this.idOrder = idOrder;
		this.idUsr = idUsr;
		this.idShop = idShop;
		this.year = year;
		this.month = month;
		this.day = day;
		this.totalPrice = totalPrice;
	}

	public String getIdOrder() {
		return idOrder;
	}

	public void setIdOrder(String idOrder) {
		this.idOrder = idOrder;
	}

	public String getIdUsr() {
		return idUsr;
	}

	public void setIdUsr(String idUsr) {
		this.idUsr = idUsr;
	}

	public String getIdShop() {
		return idShop;
	}

	public void setIdShop(String idShop) {
		this.idShop = idShop;
	}

	public int getYear() {
		return year;
	}

	public void setYear(int year) {
		this.year = year;
	}

	public int getMonth() {
		return month;
	}

	public void setMonth(int month) {
		this.month = month;
	}

	public int getDay() {
		return day;
	}

	public void setDay(int day) {
		this.day = day;
	}

	public float getTotalPrice() {
		return totalPrice;
	}

	public void setTotalPrice(float totalPrice) {
		this.totalPrice = totalPrice;
	}

	@Override
	public String toString() {
		return "Ordered [idOrder=" + idOrder + ", idUsr=" + idUsr + ", idShop="
				+ idShop + ", year=" + year + ", month=" + month + ", day="
				+ day + ", totalPrice=" + totalPrice + "]";
	}

	public String toOutputString(String seperator, String nexLine,
			String dateSeperator) {
		return getIdOrder() + seperator + getIdUsr() + seperator + getIdShop()
				+ seperator + getYear() + dateSeperator + getMonth()
				+ dateSeperator + getDay() + seperator + getTotalPrice()
				+ nexLine;
	}
}
