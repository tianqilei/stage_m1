package entityOfPostgresql;

public class Visit {

	private String idUsr;
	private String idItem;
	private char buy;
	private int year;
	private int month;
	private int day;
	private int hour;
	private int minute;
	private int second;

	public Visit(String idUsr, String idItem, char buy, int year, int month,
			int day, int hour, int minute, int second) {
		super();
		this.idUsr = idUsr;
		this.idItem = idItem;
		this.buy = buy;
		this.year = year;
		this.month = month;
		this.day = day;
		this.hour = hour;
		this.minute = minute;
		this.second = second;
	}

	public String getIdUsr() {
		return idUsr;
	}

	public void setIdUsr(String idUsr) {
		this.idUsr = idUsr;
	}

	public String getIdItem() {
		return idItem;
	}

	public void setIdItem(String idItem) {
		this.idItem = idItem;
	}

	public char getBuy() {
		return buy;
	}

	public void setBuy(char buy) {
		this.buy = buy;
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

	public int getHour() {
		return hour;
	}

	public void setHour(int hour) {
		this.hour = hour;
	}

	public int getMinute() {
		return minute;
	}

	public void setMinute(int minute) {
		this.minute = minute;
	}

	public int getSecond() {
		return second;
	}

	public void setSecond(int second) {
		this.second = second;
	}

	@Override
	public String toString() {
		return "Visit [idUsr=" + idUsr + ", idItem=" + idItem + ", buy=" + buy
				+ ", year=" + year + ", month=" + month + ", day=" + day
				+ ", hour=" + hour + ", minute=" + minute + ", second="
				+ second + "]";
	}

	public String toOutputString(String seperator, String nextLine,
			String dateSeperator, String timeSeperator) {
		return getIdUsr() + seperator + getIdItem() + seperator + getYear()
				+ dateSeperator + getMonth() + dateSeperator + getDay()
				+ seperator + getHour() + timeSeperator + getMinute()
				+ timeSeperator + getSecond() + seperator + getBuy() + nextLine;
	}
}
