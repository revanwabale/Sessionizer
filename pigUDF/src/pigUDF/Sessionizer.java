package pigUDF;
import java.io.IOException;
import java.util.Iterator;

import org.apache.pig.EvalFunc;
import org.apache.pig.PigWarning;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
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

public class Sessionizer extends EvalFunc<DataBag>{

	static final int sessionTimeWindow = 900000; //15 minutes in mill-seconds	
	//DataBag sessionedDb;
	DataBag ipDb;
	//long ipDbSize;
	long sessionStartTs;
	Tuple db_t;
	String sessionId ;
	BagFactory bagFactory = BagFactory.getInstance();
	DataBag sessionedDb = bagFactory.newDefaultBag();


	public DataBag exec(Tuple input) throws IOException {

		
		if (input == null || input.size() == 0)
		return null;
		try{
			
			ipDb = (DataBag)input.get(0);
			
			long ipDbSize = ipDb.size();
			//System.out.println("ipDbSize --> "+ipDbSize);
		    if (ipDbSize > 1) { // if DB has tuple
		    	//get the 1st tuple TS
		    	
		    	//System.out.println("inDB has more than 1 tuple..!");

		    	Iterator<Tuple> ipDbIter = ipDb.iterator();
		    	
		    	db_t = ipDbIter.next();
		    	//sessionStartTs = (long) db_t.get(16);
		    	sessionStartTs = Long.parseLong((String) db_t.get(16));	 //position of timestamp
		    	String reqIp = (String)db_t.get(2);	    //position of reqIp
		    	
		    	//System.out.println("check1: sessionStartTs = "+sessionStartTs + ", reqIp = " +reqIp);

	    		sessionId = getSessionId(reqIp, sessionStartTs, sessionStartTs); //ts is same for 1st entry.
	    		db_t.append(sessionId);  // attach to tuple its sessionId
	    		sessionedDb.add(db_t); //1st tuple got its session id.
		    	
		    	//System.out.println("check2: sessionId = "+sessionId + ", sessionedDb = " +sessionedDb.toString());

		    	while (ipDbIter.hasNext())
		    	{
			    	//System.out.println("check3: hasNext");

		    		db_t = ipDbIter.next();
		    		long ts = Long.parseLong( (String) db_t.get(16));	 //position of timestamp
		    		reqIp = (String)db_t.get(2);	    //position of reqIp
		    		
		    		if ((ts - sessionStartTs) > sessionTimeWindow) { //larger - smaller

		    			sessionStartTs = ts;  //change in session due to windowing check with next tuple as more than 1 entry
		    		}
		    		//get sessionId for this tuple	
		    		sessionId = getSessionId(reqIp, sessionStartTs, ts);
		    		db_t.append(sessionId);  // attach to tuple its sessionId
		    		sessionedDb.add(db_t); 
		    		
			    	//System.out.println("check4: sessionId = "+sessionId + ", sessionedDb = " +sessionedDb.toString());

		    	}
		    }else if (ipDbSize > 0 && ipDbSize <= 1) {
		    	//just one entry for this ip
		    	Iterator<Tuple> ipDbIter = ipDb.iterator();
		    	db_t = ipDbIter.next();
		    	//System.out.println("db_t.get(16)" + db_t.get(16));
		    	sessionStartTs = Long.parseLong((String) db_t.get(16));
	    		long ts = Long.parseLong( (String) db_t.get(16));	 //position of timestamp
	    		String reqIp = (String)db_t.get(2);	    //position of reqIp
		    	sessionId = getSessionId(reqIp, sessionStartTs, ts);
		    	db_t.append(sessionId);  // attach to tuple its sessionId
		    	sessionedDb.add(db_t); 
		    }
		    
	    	//System.out.println("returning sessionedDb = " + sessionedDb);
	    	
	    	//warn("returning sessionedDb =!", sessionedDb);
		
		return sessionedDb;
		
		}catch(Exception e){
		throw new IOException("Caught exception processing input row ", e);
			//return sessionedDb;
		
		}
		}

	private String getSessionId(String reqIp, long sessionStartTs2, long ts) throws Exception {
		if ((ts - sessionStartTs2) <= sessionTimeWindow && (ts - sessionStartTs2) > -1)  //15 minute time window
			return(sessionStartTs2 + ".".concat(MD5(reqIp)));  //do avoid direct exposure to ipAddress:this is unique id
		throw new Exception("databag is not ordered ! "+ ts + sessionStartTs2);
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
}


