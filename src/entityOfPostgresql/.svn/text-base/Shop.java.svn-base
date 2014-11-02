package entityOfPostgresql;

public class Shop {

	private String id_shop;
	private String name;
	private String url;

	public Shop(String id_shop, String name, String url) {
		super();
		this.id_shop = id_shop;
		this.name = name;
		this.url = url;
	}

	public String getId_shop() {
		return id_shop;
	}

	public void setId_shop(String id_shop) {
		this.id_shop = id_shop;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}

	@Override
	public String toString() {
		return "Shop [id_shop=" + id_shop + ", name=" + name + ", url=" + url
				+ "]";
	}

	public String toOutputString(String seperator, String nextLine) {
		return getId_shop() + seperator + getName() + seperator + getUrl()
				+ nextLine;
	}
}
