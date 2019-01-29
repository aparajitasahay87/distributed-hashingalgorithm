import java.io.*;
import java.util.Hashtable;
import java.util.Set;

/**
 * Test program
 */
public class TestProgram {
    private final static String multicast_group = "239.255.255.255";
    
    private final static int port = 7311;

    // Partition 0
    public class WriteData0 extends JSpace.Entry {
    	public int A1 = 1;
    }
    
    public class Data0 extends JSpace.Entry {
    	public int A1;
    }
   
    // Partition 1
    public class WriteData1 extends JSpace.Entry {
    	public double U1 = 1;
    }
    
    public class Data1 extends JSpace.Entry {
    	public double U1;
    }
    
    // Partition 2
    public class WriteData2 extends JSpace.Entry {
    	public double f2 = 100000;
    } 
    
    public class Data2 extends JSpace.Entry {
    	public double f2;
    }
    
    // Partition 3
    public class WriteData3 extends JSpace.Entry {
    	public double _2 = 8282828.8;
    } 
    
    public class Data3 extends JSpace.Entry {
    	public double _2;
    } 

    public TestProgram( ) {
		JSpace.Client client = new JSpace.Client( multicast_group, port );
		
		JSpace.Entry data[] = new JSpace.Entry[]
		{
			new Data0(),
			new Data1(),
			new Data2(),
			new Data3()
		};
		
		JSpace.Entry writeData[] = new JSpace.Entry[]
		{
			new WriteData0(),
			new WriteData1(),
			new WriteData2(),
			new WriteData3()
		};
		try {
			// R = 0, W = 1, T = 2.
			for(int index = 0 ; index < data.length ; index++)
			{
				// Write, Read and Take
				System.out.println("Writing:" + writeData[index].getName());
				client.write(writeData[index]);
				System.in.read();
				
				System.out.println("read:" + data[index].getName());
				client.read(data[index]);
				
				System.in.read();
				
				
				
				System.out.println("take:" + data[index].getName());
				client.take(data[index]);
				
				System.in.read();
			} 
			
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
    }

    /**
     * The actual program starts from the main( ) and instantiates a Writer
     * object.
     *
     * @param args nothing to pass
     */

    public static void main( String[] args ) {
    	new TestProgram( );
    }
}