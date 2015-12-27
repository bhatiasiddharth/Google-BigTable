import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.hadoop.conf.Configuration;

public class MasterDriver {
	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		int port = 6605;
		final ExecutorService service = Executors.newCachedThreadPool();
		ServerSocket serverSocket = new ServerSocket(port);
		serverSocket.setReuseAddress(true);
		
        while(true){
            Socket clientSocket = serverSocket.accept();
            service.submit(new MasterService(clientSocket));
        }
	}

}
