import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.net.UnknownHostException;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;


public class ClientSocket {
	BufferedReader in;
	PrintWriter out;
	Socket socket;
	
	public ClientSocket(String host, int port) {
		super();
		setupSocket(host, port);
	}
	
	public ClientSocket(String address) {
		super();
		if(address.indexOf(":") == -1) {
			System.err.println("Invalid socket address");
		}
		String host = address.split(":")[0];
		int port = Integer.parseInt(address.split(":")[1]);
		setupSocket(host, port);
	}
	
	public void setupSocket(String host, int port) {
		try {
            this.socket = new Socket(host, port);
            this.out = new PrintWriter(socket.getOutputStream(), true);
            this.in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        } catch (UnknownHostException e) {
            System.err.println("Don't know about host " + host);
            System.exit(1);
        } catch (IOException e) {
            System.err.println("Couldn't get I/O for the connection to " +
                host);
            System.exit(1);
        }
	}
	public JsonObject send(String operation, JsonElement data) throws IOException {
		JsonObject JsonMessage = new JsonObject();
		JsonMessage.addProperty("operation", operation);
		JsonMessage.add("data", data);
		String message = Utils.gson.toJson(JsonMessage);
        out.println(message);
        JsonObject result = new JsonParser().parse(in.readLine()).getAsJsonObject();
        return result;
	}
	
	
}
