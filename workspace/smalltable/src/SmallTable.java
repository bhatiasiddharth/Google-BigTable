import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeMap;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.annotations.Expose;

/*
 * {
 * 	name: webtable,
 * 	families: ["lang", "content", "anchor"],
 * 	tablets: {
 * 		"start, end" : "tabletserver : port"
 * 	}
 * }
 * 
 */
public class SmallTable {
	public static String HOST;
	public static Integer PORT;
	@Expose String tableName;
	@Expose List<String> columnFamilies = new ArrayList<String>();
	private boolean isOpened = false;
	@Expose TreeMap<String, String> tabletMap = new TreeMap<String, String>();
	ClientSocket masterSocket;
	
	public SmallTable(String tableName) {
		super();
		this.tableName = tableName; 
		this.masterSocket = new ClientSocket(HOST, PORT);
	}
	
	public void initTable(SmallTable table) {
		this.tableName = table.tableName;
		this.columnFamilies = table.columnFamilies;
		this.tabletMap = table.tabletMap;
	}
	
	public boolean open() throws IOException {
		JsonElement tableJson = Utils.gson.toJsonTree(this);
		JsonObject jsonResponse = masterSocket.send("open", tableJson);
		boolean status = jsonResponse.get("status").getAsBoolean();
		if(status) {
			this.isOpened = true;
			JsonElement data = jsonResponse.get("data");
			SmallTable table = Utils.gson.fromJson(data, SmallTable.class);
			initTable(table);
		}else {
			System.err.println("Table does not exist");
		}
		return status;
	}
	
	public boolean update() throws IOException {
		if(!this.isOpened) {
			System.err.println("Open table to update it");
			return false;
		}
		JsonElement tableJson = Utils.gson.toJsonTree(this);
		JsonObject jsonResponse = masterSocket.send("update", tableJson);
		boolean status = jsonResponse.get("status").getAsBoolean();
		return status;
	}
	
	public boolean create() throws IOException {
		// Create default tablet entry
		tabletMap = Cluster.getTabletMap();
		JsonElement tableJson = Utils.gson.toJsonTree(this);
		JsonObject jsonResponse = masterSocket.send("create", tableJson);
		boolean status = jsonResponse.get("status").getAsBoolean();
		if(status) this.isOpened = true;
		return status;
	}
	
	public boolean delete() throws IOException {
		if(!this.isOpened) {
			System.err.println("Open table to delete it");
			return false;
		}
		JsonElement tableJson = Utils.gson.toJsonTree(this);
		JsonObject jsonResponse = masterSocket.send("delete", tableJson);
		boolean status = jsonResponse.get("status").getAsBoolean();
		if(status) this.isOpened = false;
		return status;
	}
	
	public boolean addColumnFamily(String columnFamily) {
		this.columnFamilies.add(columnFamily);
		return true;
	}
	
	private String getTablet(String key) {
		if(!this.isOpened) {
			System.err.println("Table is not opened");
			return null;
		}
		System.out.println(tabletMap);
		Set<String> tablets = tabletMap.keySet();
		Iterator<String> tabletIterator = tablets.iterator();
		while(tabletIterator.hasNext()){
			String index = tabletIterator.next();
			String startKey = index.split(",")[0];
			String endKey = index.split(",")[1];
			if(key.compareTo(startKey) >= 0 && key.compareTo(endKey) < 0) {
				return tabletMap.get(index);
			}
	   }
		return null;
	}
	
	
	boolean validRow(SmallRow row) {
		Iterator<String> iterator = row.getColumnFamilies().iterator();
		while(iterator.hasNext()) {
			if(this.columnFamilies.indexOf(iterator.next()) == -1)
				return false;
		}
		return true;
	}
	
	public SmallRow getRow(String key) throws IOException {
		 //get tablet server address
		 String tabletServer = getTablet(key);
		 if(tabletServer == null) {
			 System.err.println("Tablet key not found");
			 return null;
		 }
		 
		 ClientSocket tabletSocket = new ClientSocket(tabletServer);
		 
		 //send request to tablet server
		 JsonObject rowJson = new JsonObject();
		 rowJson.getAsJsonObject().addProperty("key", key);
		 rowJson.getAsJsonObject().addProperty("table", tableName);
		 
		 JsonObject jsonResponse = tabletSocket.send("getRow", rowJson);
		 boolean status = jsonResponse.get("status").getAsBoolean();
		 if(status) {
			 JsonElement data = jsonResponse.get("data");
			 SmallRow smallRow = Utils.gson.fromJson(data, SmallRow.class);
			 return smallRow;
		 }
		 System.err.println("Key does not exist");
		 return null;
	}
	
	public boolean addRow(String key, SmallRow smallRow) throws IOException {
		 //get tablet server address
		String tabletServer = getTablet(key);
		 if(tabletServer == null) {
			 System.err.println("Tablet key not found");
			 return false;
		 }
		 
		 ClientSocket tabletSocket = new ClientSocket(tabletServer);
		 
		 if(!validRow(smallRow)) {
			 System.err.println("Row does not match table schema");
			 return false;
		 }
		 
		 //send request to tablet server
		 JsonObject rowJson = new JsonObject();
		 rowJson.getAsJsonObject().addProperty("key", key);
		 rowJson.getAsJsonObject().addProperty("table", tableName);
		 rowJson.add("value", Utils.gson.toJsonTree(smallRow));
 
		JsonObject jsonResponse = tabletSocket.send("addRow", rowJson);
		boolean status = jsonResponse.get("status").getAsBoolean();
		 return status;
	}
	
	public boolean updateRow(String key, SmallRow smallRow) throws IOException {
		  //get tablet server address
		String tabletServer = getTablet(key);
		 if(tabletServer == null) {
			 System.err.println("Tablet key not found");
			 return false;
		 }
		 
		 ClientSocket tabletSocket = new ClientSocket(tabletServer);
		 
		 if(!validRow(smallRow)) {
			 System.err.println("Row does not match table schema");
			 return false;
		 }
		 
		//send request to tablet server
		 JsonObject rowJson = new JsonObject();
		 rowJson.getAsJsonObject().addProperty("key", key);
		 rowJson.getAsJsonObject().addProperty("table", tableName);
		 rowJson.add("value", Utils.gson.toJsonTree(smallRow));

		JsonObject jsonResponse = tabletSocket.send("updateRow", rowJson);
		boolean status = jsonResponse.get("status").getAsBoolean();
		 return status;
	}
	
	public boolean deleteRow(String key) throws IOException {
		  //get tablet server address
		String tabletServer = getTablet(key);
		 if(tabletServer == null) {
			 System.err.println("Tablet key not found");
			 return false;
		 }
		 
		 ClientSocket tabletSocket = new ClientSocket(tabletServer);
		 
		 //send request to tablet server
		 JsonObject rowJson = new JsonObject();
		 rowJson.getAsJsonObject().addProperty("key", key);
		 rowJson.getAsJsonObject().addProperty("table", tableName);
		 
		 JsonObject jsonResponse = tabletSocket.send("deleteRow", rowJson);
		 boolean status = jsonResponse.get("status").getAsBoolean();
		 return status;
	}
	public boolean close() throws IOException {
		if(!this.isOpened) {
			System.err.println("Table is not opened");
			return false;
		}
		JsonElement tableJson = Utils.gson.toJsonTree(this);
		masterSocket.send("close", tableJson);
		masterSocket.socket.close();
		isOpened = false;
		return true;
	}
}
