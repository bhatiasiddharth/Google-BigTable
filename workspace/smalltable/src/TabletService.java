import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.TreeMap;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class TabletService implements Runnable{
	BufferedReader in;
	PrintWriter out;
	static String prefix = "tablet/";
	Socket socket;
    public TabletService(Socket clientSocket){
    	this.socket = clientSocket;
    }

    public void run(){
        try{
            this.out = new PrintWriter(socket.getOutputStream(), true);
            this.in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            // loop till client closes table
            String message = in.readLine();
            while(message != null) {
            	this.recv(message);
            	message = in.readLine();
            }
            socket.close();
        }
        catch(IOException e){
        	
        }
    }
    
    private String createResponse(boolean status, String data) {
    	JsonObject JsonMessage = new JsonObject();
		JsonMessage.addProperty("status", status);
		JsonElement jsonData = new JsonParser().parse(data);
		JsonMessage.add("data", jsonData);
		
		return Utils.gson.toJson(JsonMessage);
    }
    
    private String preprocess(String operation, JsonElement message) throws IOException {
    	SmallRow smallRow = Utils.gson.fromJson(message.getAsJsonObject().get("value"), SmallRow.class);
    	String key = message.getAsJsonObject().get("key").getAsString();
    	String table = message.getAsJsonObject().get("table").getAsString();
    	
    	// Add key for table in memtable
    	if(!TabletDriver.memtable.containsKey(table)) {
    		TabletDriver.memtable.put(table, new TreeMap<String, SmallRow>());
    	}
    	if(operation.equals("addRow") || operation.equals("updateRow")) {
    		// add record to memtable
    		System.out.println("creating - "  + key);
			TabletDriver.memtable.get(table).put(key, smallRow);
			return createResponse(true, "");
    	}else if(operation.equals("getRow")) {
    		// get record from memtable / disk

    		System.out.println("reading - "  + key);
    		if(TabletDriver.memtable.containsKey(table) 
    				&& TabletDriver.memtable.get(table).containsKey(key)) {
        		smallRow = TabletDriver.memtable.get(table).get(key);
        		return createResponse(true, Utils.gson.toJson(smallRow));
    		}
    		
    		smallRow = TabletDriver.searchDisk(table, key);
    		if(smallRow != null) {
    			return createResponse(true, Utils.gson.toJson(smallRow));
    		}
    		
    		return createResponse(false, "");
    	}else if(operation.equals("deleteRow")) {
    		// delete record from memtable, no need to delete from disk
    		smallRow = TabletDriver.memtable.get(table).remove(key);
    		return createResponse(smallRow != null ? true : false, "");
    	}
    	return createResponse(false, "");
	}
    
    public void recv(String message) throws IOException{
    	System.out.println(message);
		JsonObject jsonMessage = new JsonParser().parse(message).getAsJsonObject();
		String operation = jsonMessage.get("operation").getAsString();
		JsonElement data = jsonMessage.get("data");
		String response = preprocess(operation, data);
        out.println(response);
	}
	
}
