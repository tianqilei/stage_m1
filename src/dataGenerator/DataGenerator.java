package dataGenerator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import tools.BuyGenerator;
import tools.DictionaryReader;
import tools.IntGenerator;
import tools.PriceGenerator;
import tools.SexGenerator;
import tools.UrlGenerator;
import tools.Zipf;
import entityOfPostgresql.Address;
import entityOfPostgresql.Friend;
import entityOfPostgresql.Item;
import entityOfPostgresql.LineItem;
import entityOfPostgresql.Ordered;
import entityOfPostgresql.Shop;
import entityOfPostgresql.Usr;
import entityOfPostgresql.Visit;

/*
 * This class is a data generator  
 * */
public class DataGenerator {

	private static final double SKEW = 0.5;
	private static final int LENGTH_DESCRIPTION = 1;

	// private ArrayList<Friend> friendsList = new ArrayList<Friend>();
	// private ArrayList<Address> addressesList = new ArrayList<Address>();
	// private ArrayList<Visit> visitsList = new ArrayList<Visit>();
	private ArrayList<Ordered> ordersList = new ArrayList<Ordered>();
	// private ArrayList<LineItem> lineItemsList = new ArrayList<LineItem>();
	private ArrayList<Usr> usrList = new ArrayList<Usr>();
	private ArrayList<Shop> shopList = new ArrayList<Shop>();
	private ArrayList<Item> itemList = new ArrayList<Item>();
	private DictionaryReader dReader;
	private ArrayList<String> statesArrayList = new ArrayList<String>();
	private int nbStates = 0;
	private HashMap<String, ArrayList<String>> friendshipHashMap = new HashMap<String, ArrayList<String>>();
	private HashMap<String, ArrayList<Address>> addressHashMap = new HashMap<String, ArrayList<Address>>();
	private HashMap<String, ArrayList<Visit>> visitHashMap = new HashMap<String, ArrayList<Visit>>();
	private HashMap<String, ArrayList<LineItem>> itemOforderHashMap = new HashMap<String, ArrayList<LineItem>>();
	private HashMap<String, ArrayList<Item>> shopItemsHashMap = new HashMap<String, ArrayList<Item>>();
	private Integer shopIdAccu;
	private Integer usrIdAccu;
	private Integer itemIdAccu;
	private Integer orderIdAccu;
	private Zipf zipfGenerator;

	public DataGenerator() throws IOException {
		dReader = new DictionaryReader();
		zipfGenerator = new Zipf(DataGeneratorIO.getN(), SKEW);
		statesArrayList.add("Fr");
		nbStates++;
		statesArrayList.add("Ch");
		nbStates++;
		statesArrayList.add("US");
		nbStates++;
		statesArrayList.add("En");
		nbStates++;
		statesArrayList.add("Ge");
		nbStates++;
		shopIdAccu = 0;
		usrIdAccu = 0;
		itemIdAccu = 0;
		orderIdAccu = 0;
	}

	public Shop getShop() {
		String shopId = "shop" + shopIdAccu.toString();
		shopIdAccu++;
		String shopUrl = UrlGenerator.getUrl(shopId);
		Shop shop = new Shop(shopId, dReader.getRandomWord(), shopUrl);
		shopList.add(shop);
		return shop;
	}

	public Usr getUsr() {
		String usrId = "usr" + usrIdAccu.toString();
		usrIdAccu++;
		String name = dReader.getRandomWord(2);
		char sex = SexGenerator.getSex();
		int age = IntGenerator.getInt(100);
		Usr usr = new Usr(usrId, name, sex, age);
		usrList.add(usr);
		return usr;
	}

	public Item getItem() {
		String idItem = "item" + itemIdAccu.toString();
		itemIdAccu++;
		String name = dReader.getRandomWord();
		String url = UrlGenerator.getUrl(idItem);
		String description = dReader.getRandomWord(IntGenerator
				.getInt(LENGTH_DESCRIPTION));
		float price = PriceGenerator.getPrice();
		String id_shop = "shop"
				+ IntGenerator.getInt(DataGeneratorIO.NB_SHOP - 1);
		Item item = new Item(idItem, name, url, description, price, id_shop);
		itemList.add(item);
		add2shopItems(item);
		return item;
	}

	private void add2shopItems(Item item) {
		if (shopItemsHashMap.containsKey(item.getId_shop())) {
			shopItemsHashMap.get(item.getId_shop()).add(item);
		} else {
			ArrayList<Item> temp = new ArrayList<Item>();
			temp.add(item);
			shopItemsHashMap.put(item.getId_shop(), temp);
		}
	}

	public boolean checkFriends(String usrId1, String usrId2) {
		if (friendshipHashMap.containsKey(usrId1)) {
			ArrayList<String> fl = friendshipHashMap.get(usrId1);
			for (int i = 0; i < fl.size(); i++) {
				if (usrId2.equals(fl.get(i))) {
					return false;
				}
			}
			return true;
		} else {
			return true;
		}

		/*
		 * for (int i = 0; i < friendsList.size(); i++) { Friend friendExist =
		 * friendsList.get(i); if ((friendExist.getIdUsr1() == usrId1 &&
		 * friendExist.getIdUsr2() == usrId2) || (friendExist.getIdUsr1() ==
		 * usrId2 && friendExist .getIdUsr2() == usrId1)) { return false; } }
		 * return true;
		 */
	}

	/*
	 * get a "friend" where the pair (usrId1, usrId2) is unique
	 */
	public Friend getFriend() {
		// zipfGenerator = new Zipf(friendsList.size(), SKEW);
		int size = usrList.size();
		if (size == 0) {
			System.err.println("user list is empty");
			return null;
		}

		String usrId1 = getExistUsrId();
		// String usrId2 = getExistUsrIdZipf();
		String usrId2 = getExistUsrId();
		while (usrId1 == usrId2) {
			// usrId2 = getExistUsrIdZipf();
			usrId2 = getExistUsrId();
		}
		while (!checkFriends(usrId1, usrId2)) {
			// usrId2 = getExistUsrIdZipf();
			usrId2 = getExistUsrId();
			while (usrId1 == usrId2) {
				// usrId2 = getExistUsrIdZipf();
				usrId2 = getExistUsrId();
			}
		}
		Friend friend = new Friend(usrId1, usrId2);
		// friendsList.add(friend);
		add2FriendshipMap(friend);
		return friend;
	}

	/*
	 * used by GataGeneratorIO4Hbase.java. Memory the list of friends for every
	 * user by using a map
	 */
	public void add2FriendshipMap(Friend friend) {
		if (friendshipHashMap.containsKey(friend.getIdUsr1())) {
			if (!friendshipHashMap.get(friend.getIdUsr1()).add(
					friend.getIdUsr2())) {
				System.err
						.println("add into friendshipHashMap failed : DataGenerator.java");
			}
		} else {
			ArrayList<String> tempList = new ArrayList<String>();
			tempList.add(friend.getIdUsr2());
			friendshipHashMap.put(friend.getIdUsr1(), tempList);
		}

		if (friendshipHashMap.containsKey(friend.getIdUsr2())) {
			if (!friendshipHashMap.get(friend.getIdUsr2()).add(
					friend.getIdUsr1())) {
				System.err
						.println("add into friendshipHashMap failed : DataGenerator.java");
			}
		} else {
			ArrayList<String> tempList = new ArrayList<String>();
			tempList.add(friend.getIdUsr1());
			friendshipHashMap.put(friend.getIdUsr2(), tempList);
		}
	}

	/*
	 * get an unique address
	 */
	public Address getAddress() {
		String usrId = getExistUsrId();
		String street = dReader.getRandomWord();
		String city = dReader.getRandomWord();
		String country = statesArrayList.get(IntGenerator.getInt(nbStates - 1));

		Address address = new Address(usrId, street, city, country);
		while (!checkAddr(address)) {
			usrId = getExistUsrId();
			street = dReader.getRandomWord();
			city = dReader.getRandomWord();
			country = dReader.getRandomWord();
			address = new Address(usrId, street, city, country);
		}

		// addressesList.add(address);
		add2AddressHashMap(address);
		return address;
	}

	private boolean checkAddr(Address address) {
		if (addressHashMap.containsKey(address.getIdUsr())) {
			ArrayList<Address> al = addressHashMap.get(address.getIdUsr());
			for (int i = 0; i < al.size(); i++) {
				Address addrExist = al.get(i);
				if (addrExist.getCity().equals(address.getCity())
						&& addrExist.getCountry().equals(address.getCountry())
						&& addrExist.getStreet().equals(address.getStreet())) {
					return false;
				}
			}
			return true;
		} else {
			return true;
		}
	}

	private void add2AddressHashMap(Address address) {
		if (addressHashMap.containsKey(address.getIdUsr())) {
			if (!addressHashMap.get(address.getIdUsr()).add(address)) {
				System.err
						.println("add into addressHashMap failed : DataGenerator.java");
			}
		} else {
			ArrayList<Address> tempList = new ArrayList<Address>();
			tempList.add(address);
			addressHashMap.put(address.getIdUsr(), tempList);
		}
	}

	/*
	 * check if the v is unique which means the triple (usrId, itemId, date) is
	 * unique true : unique; false : not unique
	 */
	private boolean checkVisit(Visit v) {
		if (visitHashMap.containsKey(v.getIdUsr())) {
			ArrayList<Visit> vl = visitHashMap.get(v.getIdUsr());
			for (int i = 0; i < vl.size(); i++) {
				Visit visit = vl.get(i);
				if (visit.getIdItem().equals(v.getIdItem())
						&& visit.getIdUsr().equals(v.getIdUsr())
						&& visit.getYear() == v.getYear()
						&& visit.getMonth() == v.getMonth()
						&& visit.getDay() == v.getDay()) {
					return false;
				}
			}
			return true;
		} else {
			return true;
		}

		/*
		 * for (int i = 0; i < visitsList.size(); i++) { Visit visit =
		 * visitsList.get(i); if (visit.getIdItem().equals(v.getIdItem()) &&
		 * visit.getIdUsr().equals(v.getIdUsr()) && visit.getYear() ==
		 * v.getYear() && visit.getMonth() == v.getMonth() && visit.getDay() ==
		 * v.getDay()) { return false; } }
		 * 
		 * return true;
		 */
	}

	public Visit getVisit() {
		String usrId = getExistUsrId();
		String itemId = getExistItemId();
		int year = IntGenerator.getInt(14) + 2000;
		int month = IntGenerator.getInt(11) + 1;
		int day = IntGenerator.getInt(27) + 1;
		int hour = IntGenerator.getInt(23);
		int minute = IntGenerator.getInt(59);
		int second = IntGenerator.getInt(59);
		char buy = BuyGenerator.getBuy();
		Visit visit = new Visit(usrId, itemId, buy, year, month, day, hour,
				minute, second);
		while (!checkVisit(visit)) {
			visit.setIdUsr(getExistUsrId());
			visit.setIdItem(getExistItemId());
			visit.setYear(IntGenerator.getInt(14) + 2000);
			visit.setMinute(IntGenerator.getInt(12));
			visit.setDay(IntGenerator.getInt(30));
		}

		// visitsList.add(visit);
		add2VisitHashMap(visit);
		return visit;
	}

	public void add2VisitHashMap(Visit visit) {
		if (visitHashMap.containsKey(visit.getIdUsr())) {
			if (!visitHashMap.get(visit.getIdUsr()).add(visit)) {
				System.err
						.println("add into visitHashMap failed : DataGenerator.java");
			}
		} else {
			ArrayList<Visit> tempList = new ArrayList<Visit>();
			tempList.add(visit);
			visitHashMap.put(visit.getIdUsr(), tempList);
		}
	}

	public Ordered getOrdered() {
		String orderId = "order" + orderIdAccu.toString();
		orderIdAccu++;
		String usrId = getExistUsrId();
		String shopId = getExistShopId();
		int year = IntGenerator.getInt(14) + 2000;
		int month = IntGenerator.getInt(11) + 1;
		int day = IntGenerator.getInt(27) + 1;
		float totalPrice = PriceGenerator.getPrice();
		Ordered ordered = new Ordered(orderId, usrId, shopId, year, month, day,
				totalPrice);
		ordersList.add(ordered);
		return ordered;
	}

	/*
	 * check if lineItem is unique which means that the couple(orderId, itemId)
	 * is unique
	 */
	private boolean checkLineItem(LineItem lineItem) {
		if (itemOforderHashMap.containsKey(lineItem.getOrderId())) {
			ArrayList<LineItem> list = itemOforderHashMap.get(lineItem
					.getOrderId());
			for (int i = 0; i < list.size(); i++) {
				LineItem li = list.get(i);
				if (li.getItemId().equals(lineItem.getItemId())) {
					return false;
				} else {
					continue;
				}
			}
			return true;
		} else
			return true;
		/*
		 * for (int i = 0; i < lineItemsList.size(); i++) { LineItem li =
		 * lineItemsList.get(i);
		 * 
		 * if (!li.getOrderId().equals(lineItem.getOrderId())) { continue; }
		 * 
		 * if (!li.getItemId().equals(lineItem.getItemId())) { continue; }
		 * 
		 * return false;
		 * 
		 * 
		 * if (li.getOrderId().equals(lineItem.getOrderId()) &&
		 * li.getItemId().equals(lineItem.getItemId())) { return false; }
		 * 
		 * }
		 * 
		 * return true;
		 */
	}

	public LineItem getLineItem() {
		String orderId = getExistOrderId();
		String itemId = getExistItemId();
		int quantity = IntGenerator.getInt(100) + 1;
		LineItem lineItem = new LineItem(orderId, itemId, quantity);
		while (!checkLineItem(lineItem)) {
			lineItem.setOrderId(getExistOrderId());
			lineItem.setItemId(getExistItemId());
		}
		// lineItemsList.add(lineItem);
		add2ItemOfOrderHashMap(lineItem);
		return lineItem;
	}

	public void add2ItemOfOrderHashMap(LineItem lineItem) {
		if (itemOforderHashMap.containsKey(lineItem.getOrderId())) {
			itemOforderHashMap.get(lineItem.getOrderId()).add(lineItem);
		} else {
			ArrayList<LineItem> temp = new ArrayList<LineItem>();
			temp.add(lineItem);
			itemOforderHashMap.put(lineItem.getOrderId(), temp);
		}
	}

	/*
	 * get an existing user id from the list(uniform distribution)
	 */
	public String getExistUsrId() {
		return "usr" + IntGenerator.getInt(DataGeneratorIO.NB_USR - 1);
		// return usrList.get(IntGenerator.getInt(usrList.size() -
		// 1)).getId_usr();
	}

	public String getExistUsrIdZipf() {
		return "usr" + zipfGenerator.nextInt();
		// return usrList.get(zipfGenerator.nextInt()).getId_usr();
	}

	/*
	 * get an existing shop id from the list
	 */
	public String getExistShopId() {
		return "shop" + IntGenerator.getInt(DataGeneratorIO.NB_SHOP - 1);
		// return shopList.get(IntGenerator.getInt(shopList.size() - 1))
		// .getId_shop();
	}

	/*
	 * get an existing item id from the list
	 */
	public String getExistItemId() {
		return "item" + IntGenerator.getInt(DataGeneratorIO.NB_ITEM - 1);
		/*
		 * return itemList.get(IntGenerator.getInt(itemList.size() - 1))
		 * .getId_item();
		 */
	}

	public String getExistOrderId() {
		return "order" + IntGenerator.getInt(DataGeneratorIO.NB_ORDER - 1);
		/*
		 * return ordersList.get(IntGenerator.getInt(ordersList.size() - 1))
		 * .getIdOrder();
		 */
	}

	public HashMap<String, ArrayList<String>> getFriendshipMap() {
		return friendshipHashMap;
	}

	public HashMap<String, ArrayList<Address>> getAddressMap() {
		return addressHashMap;
	}

	public HashMap<String, ArrayList<Visit>> getVisitMap() {
		return visitHashMap;
	}

	public ArrayList<Usr> getUsrList() {
		return usrList;
	}

	public ArrayList<Item> getItemList() {
		return itemList;
	}

	public ArrayList<Shop> getShopList() {
		return shopList;
	}

	public ArrayList<Ordered> getOrderList() {
		return ordersList;
	}

	public HashMap<String, ArrayList<LineItem>> getItemOfOrderMap() {
		return itemOforderHashMap;
	}

	public HashMap<String, ArrayList<Item>> getShopItems() {
		return shopItemsHashMap;
	}

	public static void main(String[] args) throws IOException {
		DataGenerator dGenerator = new DataGenerator();
		System.out.println(dGenerator.getUsr().toString());
		System.out.println(dGenerator.getUsr().toString());
		System.out.println(dGenerator.getUsr().toString());
		System.out.println(dGenerator.getUsr().toString());
		System.out.println(dGenerator.getUsr().toString());
		System.out.println(dGenerator.getUsr().toString());

		System.out.println(dGenerator.getItem().toString());
		System.out.println(dGenerator.getShop().toString());

		System.out.println(dGenerator.getFriend().toString());
		System.out.println(dGenerator.getVisit().toString());
		System.out.println(dGenerator.getOrdered().toString());
		System.out.println(dGenerator.getAddress().toString());

		System.out.println(dGenerator.getLineItem().toString());
		System.out.println(dGenerator.getFriend().toString());
		System.out.println(dGenerator.getFriend().toString());
		System.out.println(dGenerator.getFriend().toString());
		System.out.println(dGenerator.getFriend().toString());
		System.out.println(dGenerator.getFriend().toString());
		System.out.println(dGenerator.getFriend().toString());
		System.out.println(dGenerator.getFriend().toString());
		System.out.println(dGenerator.getFriend().toString());
		System.out.println(dGenerator.getFriend().toString());
		System.out.println(dGenerator.getFriend().toString());
		System.out.println(dGenerator.getFriend().toString());
		System.out.println(dGenerator.getFriend().toString());
		System.out.println(dGenerator.getFriend().toString());
		System.out.println(dGenerator.getFriend().toString());
		System.out.println(dGenerator.getFriend().toString());
		System.out.println(dGenerator.getFriend().toString());
		System.out.println(dGenerator.getFriend().toString());
		System.out.println(dGenerator.getFriend().toString());
		System.out.println(dGenerator.getFriend().toString());
		System.out.println(dGenerator.getFriend().toString());

	}

}
