package JSpace;
import java.io.*;
import java.net.*;
import java.util.*;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;
import java.lang.*;


public class JSpace
{
	Connection connections[];
	public String multicast_group;
	public int port;
	Connection connection;
	boolean IsMaster = true;
	public int MAX_DATA_SIZE = 1024;
	//Create local hash table on each server			
	Hashtable<String, Object> ht = new Hashtable<String, Object>();
	public int serverId;
	public int numberOfMachines; 
	public Logger logger = Logger.getLogger("MyLog");  
    FileHandler fh;  
    
	//constructor definition for master server.
	public JSpace(Connection connections[],String multicast_group,int port)
	{
		 this.multicast_group = multicast_group;
		 this.port = port;
		 this.connections = connections;
		 this.IsMaster = true;
		 this.serverId = 0;
		 this.numberOfMachines = connections.length+1;
		 try 
		 {
			 //1.Broadcast information from server thread to each slave thread
			 for(int i = 0 ; i < connections.length ; i++)
			 {
				 connections[i].out.writeObject(multicast_group);
				 connections[i].out.writeObject(port);
				 connections[i].out.writeObject(numberOfMachines);
				 int id = i + 1;
				 connections[i].out.writeObject(id);
				 connections[i].out.flush();
			 }
			 
			//2.Master server create persistent thread to listen to the client.
			PersistentThread persistentThread = new PersistentThread(port,this);
			persistentThread.start();
			
			//3. Master thread read "show" and "quit" message from master server
			//Server admin can type show or quit from the keyboard.
			boolean quit = false;
			while(!quit)
			{
				// Step #1 : Read messages from admin console
				BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
				String line = br.readLine();
				switch (line)
				{
				case "show":
					 
					for(int i = 0 ; i < connections.length ; i++)
					{
						connections[i].out.writeObject(line);
						connections[i].out.flush();
					}
					//Receives message from each slave
					StringBuilder sb = new StringBuilder();
					// First master will put its own data
					sb.append(serializeHashData());
					// Step#2 : Read messages from slaves
					for(int i = 0 ; i < connections.length ; i++)
					{
						try
						{
							String slaveHt = (String)connections[i].in.readObject();
							System.out.println("Received message fron slave " + i + slaveHt);
							if( slaveHt != null)
							{
								sb.append(slaveHt);
							}
						}
						catch(EOFException ex)
						{
						}
					}
					
					System.out.println(sb.toString());
					break;
				case "quit" :

					//send message to each slave server  
					for(int i = 0 ; i < connections.length ; i++)
					{
						connections[i].out.writeObject(line);
						connections[i].out.flush();
					}
					
					
					break;
				default:
					System.out.println("Didn't understand line:" + line);
					break;
				}
			}
			
			System.out.println("quitting");
		 }
		 catch (Exception ex)
		 {
			 System.out.println(ex);
		 }
	}
	
	//constructor definition for slave server
	public JSpace(Connection connection)
	{
		try
		{
			
			this.connection = connection;
			this.IsMaster = false;
			
			//1. Slave server reads output stream of the master
			this.multicast_group = (String)connection.in.readObject();
			this.port = (int)connection.in.readObject();
			this.numberOfMachines = (int)connection.in.readObject();
			String numberofMachines = "Total no. of slave machines" + numberOfMachines ;
			this.serverId = (int)connection.in.readObject();
			String serverid = "Server is" + serverId ;
			
			 //This block configure the logger with handler and formatter  
	        fh = new FileHandler("/net/metis/home/sahay87/LogFile" + this.serverId + ".log");  
	        logger.addHandler(fh);
	        SimpleFormatter formatter = new SimpleFormatter();  
	        fh.setFormatter(formatter);  
			
	        // the following statement is used to log any messages  
	        logger.info("My first log");  
	        
			//2. Slave server create persistent thread to listen to the client.
			PersistentThread persistentThread = new PersistentThread(port,this);
			persistentThread.start();
			
			//3.Reads master response at main thread of each slave server
			boolean quit = false;
			while(!quit)
			{
				String userInput = (String) connection.in.readObject();
				this.logger.info("Received message:" + userInput);

				switch(userInput)
				{
					case "show":
						//Send it's local hash table to master server 
						connection.out.writeObject(serializeHashData());
						this.logger.info("HashTable Data" + serializeHashData());
						connection.out.flush();
						break;
					case "quit":
						
						//connection.out.writeObject("closeConnectionWithMe");
						connection.out.flush();
						quit = true;
						//System.exit(-1);
						connection.close();
						
						break;
					default:
						this.logger.info( "Not recognized message:" + userInput);
						break;
				}
			}
		}
		catch(Exception ex)
		{
			logger.info(ex.toString());
			//for quitting
			//System.exit(-1);
		}
	}
	
	//Convert hash table to string
	private String serializeHashData()
	{
		return this.ht.toString();
	}
	
	//Read hash table content
	 public synchronized Object Read(String key)
	 {
		 Object result = this.ht.get(key);
		 return result;
	 }
	 
	 //Write into hash table as per client request
	 public synchronized void Write(String key, Object value)
	 {
		 this.ht.put(key, value);
	 }
	 
	 //Reads data and then removes the content from hash table
	 public synchronized Object Take(String key)
	 {
		 Object result = this.ht.remove(key);
		 return result;
	 }
}	
	   
//Thread that Listens to Client request
class PersistentThread extends Thread 
{
	private int port;
	private JSpace jspace;
	private char validChars[] = new char[54];
	
	public PersistentThread(int port,JSpace Jspace)
	{
		this.port = port;
		this.jspace = Jspace;
		//Preparing data: number of valid characters in a variable name.
		//used for hashing algorithm.
		for(int index = 0;index < 26 ; index++)
		{
			validChars[index] = (char)('A' + index);
		}
		
		for(int index = 26;index < 52 ; index++)
		{
			validChars[index] = (char)('a' + index - 26);
		}
		
		validChars[52] = '$';
		validChars[53] = '_';
	}
	
	public void run() 
	{
		try
		{
			//this.jspace.logger.info("Hello from persistent thread!");
			//1. Listens to UDP broadcast connection from ant client.
			InetAddress group 
			= InetAddress.getByName(this.jspace.multicast_group  );
		    MulticastSocket socket = null;
		    try
		    { 
		    	socket = new MulticastSocket( port );
		    
			    socket.joinGroup( group );
	
			    while ( true ) {
			    	
					byte[] buf = new byte[this.jspace.MAX_DATA_SIZE];
					DatagramPacket p 
					    = new DatagramPacket( buf, buf.length );
					socket.receive( p );
					
					Entry data = Entry.deserialize(buf);
					
					//this.jspace.logger.info("Received data:" + this.jspace.serverId + " " + data.getName());
					//this.jspace.logger.info("P data:" + this.jspace.serverId);
					if(CheckHashTable(data))
					{
						//this.jspace.logger.info("Processing data:" + this.jspace.serverId);
						
						
						// Send to per request
						PerRequestThread perRequest = new PerRequestThread(this.jspace, data, p.getAddress(), p.getPort());
						perRequest.start();
						
					}
					else
					{
						//this.jspace.logger.info("Not Processing data:" + this.jspace.serverId);
					}
			    }
		    }
		    finally
		    {
		    	if(socket != null && 
    			   !socket.isClosed())
		    	{
		    		socket.close();
		    	}
		    }
		}
		catch(Exception ex)
		{
			this.jspace.logger.info(ex.toString());
		}
	}
	
	//Check the local hash table data range with the client request data
	//Returns true if range exist.
	boolean CheckHashTable(Entry data)
	{
		String dataName = data.getName();
		int numberOfCharacter = 54; // Valid variables name in Java starts A-Z or a-z or $ or _
		int partitionSize = (int)Math.ceil(numberOfCharacter * 1.0 / this.jspace.numberOfMachines);
		int startIndex = partitionSize * this.jspace.serverId;
		int endIndex = startIndex + partitionSize;		
		char firstChar = dataName.charAt(0);
		
		for( int index = startIndex; index < endIndex && index < validChars.length; index++)
		{
			if(validChars[index] == firstChar )
			{
				return true;
			}
		}
		 
		return false;
	}	
}
	   
//Thread created process data in a hash table. 
//Performs read , write and take method.
class PerRequestThread extends Thread
{
	private JSpace jspace;
	private Entry entry;
	private InetAddress address;
	int port;
	int groupPort = 7311;
	
	public PerRequestThread(JSpace jspace, Entry entry, InetAddress address, int port)
	{
		this.jspace = jspace;
		this.entry = entry;
		this.address = address;
		this.port = port;
	}
	
	public void run()
	{
		//perrequest thread is called only by Persistent thread.
		//this.jspace.logger.info("Hello from perRequestThread thread!");
		String operation = "Operation is" + this.entry.getOperation() + ":" + this.entry.getType() ;
		//this.jspace.logger.info(operation);
		Object result = null;
		try
		{
			// R = 0, W = 1, T = 2.
			switch(this.entry.getOperation())
			{
			case 0:
				result = this.jspace.Read(this.entry.getName());
				if( result != null)
				{
					
					this.entry.setValueOverride(result);
					
				}
				
				this.SendResult(this.entry);
				break;
			case 1:
				this.jspace.Write(this.entry.getName(), this.entry.getValue());
				
				break;
			case 2:
				result = this.jspace.Take(this.entry.getName());
				if( result != null)
				{
					this.entry.setValueOverride(result);
					//this.entry.setValue(result);
				}
				//this.jspace.logger.info(this.jspace.ht.toString());
				this.SendResult(this.entry);
				break;
			default:
				this.jspace.logger.info("Unknown operation type" + this.entry.getOperation());
			}
		}
		catch(Exception ex)
		{
			this.jspace.logger.info(ex.toString());
		}
	}
	
	
	//method to send response from server back to client.
	private void SendResult(Entry result)
	{
		DatagramSocket socket = null;
		
		try
		{
			//this.jspace.logger.info("Sending data" + result.toString() + ":" + this.address.toString() + ":" + this.port);
			socket = new DatagramSocket();
			byte[] buf = Entry.serialize(result);
			
			DatagramPacket p = new DatagramPacket( 
								buf, 
								buf.length,
								this.address,
								this.groupPort);
			socket.send(p);
			
			//this.jspace.logger.info("Sent data" + result.toString() + ":" + this.address.toString() + ":" + this.groupPort + ":" + p.getPort());
		}
		catch(Exception exception)
		{
			this.jspace.logger.info(exception.toString());
		}
		finally
		{
			if( socket != null && !socket.isClosed())
			{
				socket.close();
			}
		}
		
	}
}
