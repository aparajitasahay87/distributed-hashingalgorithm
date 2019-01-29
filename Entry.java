
package JSpace;
import java.io.*;
import java.lang.reflect.*;

/**
 * JSpace.Entry is a base object that can be written into, read from, and 
 * taken out from JSpace, (i.e. our JavaSpace implementation). A JSpace
 * user application should extend JSpace.Entry to their own objects so as
 * to maintain them in JSpace. A user application does not have to call
 * any JSpace.Entry functions, but JSpace.Client needs to use these 
 * public functions to extract only a set of variable type, name, value, 
 * and operation.
 *
 * @author  Munehiro Fukuda
 * @version %I% %G%
 * @since   1.0
 */

public class Entry implements Serializable {
    // Attributes
    private String varType; // a key to send this entry to a JSpace server
    private String varName; // a key to store this entry into a server hash
    private int operation;  // JavaSpace Op: 0 = read, 1 = write, 2 = take
    private Object value;  // a user-define value

    /**
     * getBase() sets a given user object's variable type, name, and object as 
     * well as a JavaSpace operation. It is called from read, write, and take.
     *
     * @param type   boolean, byte, char, short, int, long, float, double, or
     *               java.lang.String
     * @param name   a user-given variable name
     * @param op     a Java Space operation: read, write, or take
     * @param value  Boolean, Byte, Char, Short, Integer, Long, Float, Double,
     *               String, or null (in read or take)
     * @return       a new entry that includes a set of datatype, variable 
     *               name, value, and JSpace operation
     */
    public Entry getBase( ) {
	Class myClass = this.getClass( );
	Field[] myFields = myClass.getDeclaredFields( );// extract a data field
	for ( int i = 0; i < myFields.length; i++ ) {
	    // just examin the very 1st field only.
	    varType = myFields[i].getType( ).getName( );// extract the datatype
	    if ( varType.equals( "java.lang.String" ) )
		varType = "string";
	    varName = myFields[i].getName( ); // extract the variable name

	    try { // extract the value that this variable contains
		if ( varType.equals( "byte" ) )
		    value = new Byte( myFields[i].getByte( this ) );
		else if ( varType.equals( "char" ) )
		    value = new Character( myFields[i].getChar( this ) );
		else if ( varType.equals( "short" ) )
		    value = new Short( myFields[i].getShort( this ) );
		else if ( varType.equals( "int" ) )
		    value = new Integer( myFields[i].getInt( this ) );
		else if ( varType.equals( "long" ) )
		    value = new Long( myFields[i].getLong( this ) );
		else if ( varType.equals( "float" ) )
		    value = new Float( myFields[i].getFloat( this ) );
		else if ( varType.equals( "double" ) )
		    value = new Double( myFields[i].getDouble( this ) );
		else if ( varType.equals( "string" ) )
		    value = myFields[i].get( this );
	    } catch( Exception e ) {
		e.printStackTrace( );
		    System.exit( -1 );
	    }
	    break;
	}

	Entry entry = new Entry( );  // now create an independent base class
	// sotre the datatype, variable name, value, and JSpace operation.
	entry.varType = this.varType;   
	entry.varName = this.varName;
	entry.operation = this.operation;
	entry.value = this.value;
	
	return entry;
    }
    
    public void setValueOverride( Object value ) {
    	this.value = value;
    }

    /**
     * getBase() sets a given user object's variable type, name, and object as 
     * well as a JavaSpace operation. It is called from read, write, and take.
     *
     * @param value an object whose value will be set into this entry's data
     *        field. 
     */
    public void setValue( Object value ) {
	Class myClass = this.getClass( );
	Field[] myFields = myClass.getDeclaredFields( ); // extract data field.
	for ( int i = 0; i < myFields.length; i++ ) {
	    // just examin the very 1st field only.
	    varType = myFields[i].getType( ).getName( ); // extract datatype.
	    if ( varType.equals( "java.lang.String" ) )
		varType = "string";
	    
	    try { // based on the datatype found, we now cast a given value
		  // to the corresponding data type and set the value to
		  // the corresponding data field.
		if ( varType.equals( "byte" ) )
		    myFields[i].setByte( this, ((Byte)value).byteValue( ) );
		else if ( varType.equals( "char" ) )
		    myFields[i].setChar( this, ((Character)value).charValue());
		else if ( varType.equals( "short" ) )
		    myFields[i].setShort( this, ((Short)value).shortValue( ) );
		else if ( varType.equals( "int" ) )
		    myFields[i].setInt( this, ((Integer)value).intValue( ) );
		else if ( varType.equals( "long" ) )
		    myFields[i].setLong( this, ((Long)value).longValue( ) );
		else if ( varType.equals( "float" ) )
		    myFields[i].setFloat( this, ((Float)value).floatValue( ) );
		else if ( varType.equals( "double" ) )
		    myFields[i].setDouble( this,((Double)value).doubleValue());
		else if ( varType.equals( "string" ) )
		    myFields[i].set( this, value );
	    } catch( Exception e ) {
		e.printStackTrace( );
		System.exit( -1 );
	    }
	    break;
	}
    }

    /**
     * print() prints out this entry's datatype, variable name, and value.
     */
    public void print( ) {
	System.out.print( "type = " + varType + ", name = " + varName +
			  ", value = " );

	if ( value == null )
	    System.out.println( "null" );
	else if ( varType.equals( "byte" ) )
	    System.out.println(((Byte)value).byteValue());
	else if ( varType.equals( "char" ) )
	    System.out.println(((Character)value).charValue());
	else if ( varType.equals( "short" ) )
	    System.out.println(((Short)value).shortValue());
	else if ( varType.equals( "int" ) )
	    System.out.println(((Integer)value).intValue());
	else if ( varType.equals( "long" ) )
	    System.out.println(((Long)value).longValue());
	else if ( varType.equals( "float" ) )
	    System.out.println(((Float)value).floatValue());
	else if ( varType.equals( "double" ) )
	    System.out.println(((Double)value).doubleValue());
	else if ( varType.equals( "string" ) )
	    System.out.println((String)value);
    }

    /**
     * serialize() serializes a given entry into a byte array so that the
     * array can be passed to a UDP socket.
     *
     * @param  entry an entry to be serialized into a byte array.
     * @return       a byte array that serialized a given entry.
     */
    public static  byte[] serialize( Entry entry ) throws IOException {
	ByteArrayOutputStream out = new ByteArrayOutputStream( );
        ObjectOutputStream os = new ObjectOutputStream( out );
        os.writeObject( entry );
        return out.toByteArray( );
    }

    /**
     * deserialize() reconstruct a new entry from a given byte array.
     *
     * @param buf a byte array to be deserialized back to a new entry
     * @return    a new entry that was deseralized from a gvien array, i.e.
     *            buf[].
     */
    public static Entry deserialize( byte[] buf )
        throws IOException, ClassNotFoundException {
        ByteArrayInputStream in = new ByteArrayInputStream( buf );
	ObjectInputStream is = new ObjectInputStream( in );
        return ( Entry )is.readObject();
    }

    /**
     * getType() returns the variable type of this entry.
     * @return the variable type of this entry
     */
    public String getType( ) { return varType; }

    /**
     * getName() returns the variable name of this entry.
     * @return the variable name of this entry
     */
    public String getName( ) { return varName; }

    /**
     * getOperation() returns the operation type of this entry.
     * @return the operation type of this entry: R = 0, W = 1, T = 2.
     */
    public int getOperation( ) { return operation; }

    /**
     * setOperation() sets the operation tpe of this entry.
     * @param op the operation type to be set.
     */
    public void setOperation( int op ) { this.operation = op; }

    /**
     * getvalue() returns an object that contains the value set to this entry
     * @return an object that contains the value set to this entry.
     */
    public Object getValue( ) { return value; }
}