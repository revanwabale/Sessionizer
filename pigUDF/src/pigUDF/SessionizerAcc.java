package pigUDF;


import java.io.IOException;
import java.util.Iterator;
import java.util.UUID;

import org.apache.pig.Accumulator;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.joda.time.DateTime;
import org.joda.time.Period;
import org.apache.commons.logging.Log;

/* orders the tuples by time desc.
 * ip1, {(t1, url1, ),(t2, url1, ),(t3, url2),...etc} or
 * ip2, {(t1, url1, ),(t2, url1, ),(t3, url2),...etc} or
 * ip3, {(t1, url1, ),(t2, url2, ),(t3, url3),...etc} or
 */
/*
 *   Timestamp:chararray,
     ELBName :chararray,
     RequestIP :chararray,
     RequestPort:int,
     BackendIP:chararray,
     BackendPort :int,
     RequestProcessingTime : double,
     BackendProcessingTime : double,
     ClientResponseTime : double,
     ELBResponseCode :chararray,
     BackendResponseCode :chararray,
     ReceivedBytes:long,
     SentBytes : long,
     RequestVerb :chararray,
     URL :chararray,
     Protocol :chararray,
     unixTime: long)
 */

public class SessionizerAcc extends EvalFunc<DataBag> implements Accumulator<DataBag> {

	private final int sessionTimeWindow;
	private DataBag sessionedDb;
	private DateTime last_date; 
	private String id; 

	  public SessionizerAcc(String windowTime) 
	  { 
	    
	    Period p = new Period("PT" + windowTime.toUpperCase()); 
	    this.sessionTimeWindow = p.toStandardSeconds().getSeconds() * 1000;  // convert to millis
	 
	    cleanup();  //cleanup all object before use.
	  } 
	
	public DataBag exec(Tuple input) throws IOException {
		
	    accumulate(input);  // logic to attach the session id
	    DataBag outputBag = getValue();    //return the output bag in the last
	    cleanup();    // clean up the object of spill occur at reducer side due to accumulate
	 
	    return outputBag;   //return to pig.
	
		}

	
	public String MD5(String md5) {
		   try {
		        java.security.MessageDigest md = java.security.MessageDigest.getInstance("MD5");
		        byte[] array = md.digest(md5.getBytes());
		        StringBuffer sb = new StringBuffer();
		        for (int i = 0; i < array.length; ++i) {
		          sb.append(Integer.toHexString((array[i] & 0xFF) | 0x100).substring(1,3));
		       }
		        return sb.toString();
		    } catch (java.security.NoSuchAlgorithmException e) {
		    }
		    return null;
		}

	@Override
	public void accumulate(Tuple input) throws IOException {

		   for (Tuple t : (DataBag) input.get(0)) { 
			      String timeString = (String)t.get(0); 
			      DateTime date = new DateTime(timeString); 
			 
			      if (this.last_date == null) 
			        this.last_date = date; 
			      else if (date.isAfter(this.last_date.plus(this.sessionTimeWindow))) 
			        this.id = UUID.randomUUID().toString(); 
			      else if (date.isBefore(last_date)) 
			        throw new IOException(String.format("input time series is not sorted (%s < %s)", date, last_date)); 
			 
			      Tuple t_new = TupleFactory.getInstance().newTuple(t.getAll()); 
			      t_new.append(this.id); 
			      sessionedDb.add(t_new); 
			       
			      this.last_date = date; 
		   }
	}

	@Override
	public void cleanup() {
		    
		    this.last_date = null; 
		    this.sessionedDb = BagFactory.getInstance().newDefaultBag(); 
		    this.id = UUID.randomUUID().toString(); 
	}

	@Override
	public DataBag getValue() {
		
		return sessionedDb; 
	}


//copied from datafu udf library to avoid more time  spending 
///http://www.javased.com/?source_dir=datafu/src/java/datafu/pig/sessions/Sessionize.java
@Override 
public Schema outputSchema(Schema input) 
{ 
  try { 
    Schema.FieldSchema inputColSchema = input.getField(0); 

    if (inputColSchema.type != DataType.BAG) 
    { 
      throw new RuntimeException("Expected BAG as input to " + SessionizerAcc.class.getName()); 
    } 
     
    Schema inputBagSchema = inputColSchema.schema; 
     
    if (inputBagSchema.getField(0).type != DataType.TUPLE) 
    { 
      throw new RuntimeException(String.format("Expected input bag to contain a TUPLE, but instead found %s", 
                                               DataType.findTypeName(inputBagSchema.getField(0).type))); 
    } 
     
    Schema inputTupleSchema = inputBagSchema.getField(0).schema; 
     
    if (inputTupleSchema.getField(0).type != DataType.CHARARRAY) 
    { 
      throw new RuntimeException(String.format("Expected first element of tuple to be a CHARARRAY, but instead found %s", 
                                               DataType.findTypeName(inputTupleSchema.getField(0).type))); 
    } 
     
    Schema outputTupleSchema = inputTupleSchema.clone(); 
    outputTupleSchema.add(new Schema.FieldSchema("session_id", DataType.CHARARRAY));       
     
    return new Schema(new Schema.FieldSchema(getSchemaName(this.getClass() 
                                                           .getName() 
                                                           .toLowerCase(), input), 
                                         outputTupleSchema, 
                                         DataType.BAG)); 
  } 
  catch (CloneNotSupportedException e) { 
    throw new RuntimeException(e); 
  } 
  catch (FrontendException e) { 
    throw new RuntimeException(e); 
  } 
} 

}

