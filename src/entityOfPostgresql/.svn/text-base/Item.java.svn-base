package entityOfPostgresql;

public class Item {

	private String id_item;
	private String name;
	private String url;
	private String description;
	private float price;
	private String id_shop;

	public Item(String id_item, String name, String url, String description,
			float price, String id_shop) {
		super();
		this.id_item = id_item;
		this.name = name;
		this.url = url;
		this.description = description;
		this.price = price;
		this.id_shop = id_shop;
	}

	public String getId_shop() {
		return id_shop;
	}

	public void setId_shop(String id_shop) {
		this.id_shop = id_shop;
	}

	public String getId_item() {
		return id_item;
	}

	public void setId_item(String id_item) {
		this.id_item = id_item;
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

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	public float getPrice() {
		return price;
	}

	public void setPrice(float price) {
		this.price = price;
	}

	@Override
	public String toString() {
		return "Item [id_item=" + id_item + ", name=" + name + ", url=" + url
				+ ", description=" + description + ", price=" + price + "]";
	}

	public String toOutputString(String seperator, String nextLine) {
		return getId_item() + seperator + getName() + seperator + getPrice()
				+ seperator + getUrl() + seperator + getDescription()
				+ seperator + getId_shop() + nextLine;
	}
}
