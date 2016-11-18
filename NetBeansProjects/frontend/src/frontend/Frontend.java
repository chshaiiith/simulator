/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package frontend;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.*;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author Chetan
 */
public class Frontend {
    /**
     * @param args the command line arguments
     */
    public final static int THREAD_POOL_SIZE = 500;
    
    public static Map<Long,Long> myMap = new ConcurrentHashMap<Long,Long>();
    public final static ExecutorService pool = Executors.newFixedThreadPool(THREAD_POOL_SIZE);
    
    public static int total_thread = 0;
    
    
    private static class Simplethread implements Runnable {
        private Long key;
        private Long value = null;
        private Socket socket = null;

        private Simplethread(Socket socket) {
            this.socket = socket;
 
        }
      
        public void run() {
            try {
                String data;
                DataInputStream dataStream;
                dataStream = new DataInputStream(this.socket.getInputStream());
                
                while(true)
                {
                    data = dataStream.readUTF();
                    String [] parts = data.split(":");
                    
                    if (parts[0].equals("STOP")) {
                        // suspend the thread
                        return;
                    }
                    this.key = Long.parseLong(parts[0], 10);
                    
                    if (parts[1].equals("null")) {
                        this.value = null;
                    }
                    else {
                        this.value = Long.parseLong(parts[1], 10);
                    }
                    
                    DataOutputStream output=new DataOutputStream(socket.getOutputStream());    
                    
                    if (this.value != null) {
                        myMap.put(this.key, this.value);
                        output.writeUTF("0");
                        output.flush();
                        continue;
                    }
                    
                    if (!myMap.containsKey(this.key)) {
                        output.writeUTF("-1");
                        output.flush();
                        continue;
                    }
                    
                    output.writeUTF(Long.toString(myMap.get(this.key)));
                    output.flush();
   
                }   
            } 
            catch (IOException ex) {
                Logger.getLogger(Frontend.class.getName()).log(Level.SEVERE, null, ex);
                
            }
        }
    };
    
  
    public static void main(String[] args) throws IOException {
    
        // TODO code application logic here   
        ServerSocket listener = new ServerSocket(10000);
        try {
            while (true) {
                Socket socket = listener.accept();
                pool.execute(new Simplethread(socket));
            }
        }
        finally {
            listener.close();
        }
    } 
}
