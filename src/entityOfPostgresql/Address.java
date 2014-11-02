package entityOfPostgresql;

public class Address {

	private String idUsr;
	private String street;
	private String city;
	private String country;

	public Address(String idUsr, String street, String city, String country) {
		super();
		this.idUsr = idUsr;
		this.street = street;
		this.city = city;
		this.country = country;
	}

	public String getIdUsr() {
		return idUsr;
	}

	public void setIdUsr(String idUsr) {
		this.idUsr = idUsr;
	}

	public String getStreet() {
		return street;
	}

	public void setStreet(String street) {
		this.street = street;
	}

	public String getCity() {
		return city;
	}

	public void setCity(String city) {
		this.city = city;
	}

	public String getCountry() {
		return country;
	}

	public void setCountry(String country) {
		this.country = country;
	}

	@Override
	public String toString() {
		return "Address [idUsr=" + idUsr + ", street=" + street + ", city="
				+ city + ", country=" + country + "]";
	}

	public String toOutputString(String seperator, String nextLine) {
		return getStreet() + seperator + getCity() + seperator + getCountry()
				+ seperator + getIdUsr() + nextLine;
	}
}
