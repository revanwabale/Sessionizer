package pigUDFTest;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.Test;

import pigUDF.Sessionizer;
import pigUDF.SessionizerAcc;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DefaultTuple;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import static org.junit.Assert.assertTrue;

class SessionizerTest {

	@Test
	void tesisEvalValid() {
		assertTrue(new Sessionizer() instanceof EvalFunc);
		assertTrue(new SessionizerAcc("15m") instanceof EvalFunc); // millisecond time in int
		//fail("Not yet implemented");
	}
	
	@Test	
	void sessionizerTest_1Tuple() throws IOException {
		List<String> myList = new ArrayList<String>(Arrays.asList("2015-07-22T17:42:04.834900Z","marketpalce-shop","1.23.0.6","28165","10.0.4.227","80","2.4E-5","0.010813","2.1E-5","200","200","0","254","GET","https://paytm.com:443/papi/v1/promosearch/product/11148776/offers?parent_id=11148811&price=3399&channel=web&version=2","HTTP/1.1","1437586924834"));
		//List<String> myList = new ArrayList<String>(Arrays.asList("2015-07-22T17:42:04.834900Z","marketpalce-shop","1.23.0.6","28165","10.0.4.227","80","2.4E-5","0.010813","2.1E-5","200","200","0","254","GET","https://paytm.com:443/papi/v1/promosearch/product/11148776/offers?parent_id=11148811&price=3399&channel=web&version=2","HTTP/1.1","1437586924834"));

		//TupleFactory mTupleFactory = TupleFactory.getInstance();
		//Tuple outerTuple = mTupleFactory.newTuple(1);

		Tuple t = new DefaultTuple();
		BagFactory bagFactory = BagFactory.getInstance();
		DataBag inDb = bagFactory.newDefaultBag();
		DataBag outDb = bagFactory.newDefaultBag();
		Tuple t2 = new DefaultTuple();

		
		int i=0;
		while (i < 17) {
		t.append(myList.get(i));
		t2.append(myList.get(i));
		i++;
		}
		System.out.println("tuple size --> " + t.size());
		//System.out.println(t.toString());
	    inDb.add(t);
	    
	    t2.append("1437586924834.08185029ad71435639949446b9458edb");
	    outDb.add(t2);
	    
	    System.out.println(" after tuple size t2--> " + t2.size());
	    System.out.println(" after tuple size t--> " + t.size());
	    
	    DefaultTuple input = new DefaultTuple();
	    input.append(inDb);
	    
	    System.out.println("input="+input.toString());
	    System.out.println("outDb="+outDb.toString());

	    
	    //assertEquals(outDb, new Sessionizer().exec(input));
	    assertEquals(outDb, new SessionizerAcc("15m").exec(input));
	}
	
	
	@Test	
	void sessionizerTest_2Tuple() throws IOException {
		List<String> myList = new ArrayList<String>(Arrays.asList("2015-07-22T17:42:04.834900Z","marketpalce-shop","1.23.0.6","28165","10.0.4.227","80","2.4E-5","0.010813","2.1E-5","200","200","0","254","GET","https://paytm.com:443/papi/v1/promosearch/product/11148776/offers?parent_id=11148811&price=3399&channel=web&version=2","HTTP/1.1","1437586924834"));
		//List<String> myList = new ArrayList<String>(Arrays.asList("2015-07-22T17:42:04.834900Z","marketpalce-shop","1.23.0.6","28165","10.0.4.227","80","2.4E-5","0.010813","2.1E-5","200","200","0","254","GET","https://paytm.com:443/papi/v1/promosearch/product/11148776/offers?parent_id=11148811&price=3399&channel=web&version=2","HTTP/1.1","1437586924834"));

		TupleFactory mTupleFactory = TupleFactory.getInstance();
		Tuple outerTuple = mTupleFactory.newTuple(1);

		
		BagFactory bagFactory = BagFactory.getInstance();
		DataBag inDb = bagFactory.newDefaultBag();
		DataBag outDb = bagFactory.newDefaultBag();
		DefaultTuple input = new DefaultTuple();
		
		int j = 0;

		
		while (j < 2) {
			Tuple t = new DefaultTuple();
			Tuple t2 = new DefaultTuple();
		
		int i=0;
		while (i < 17) {
		t.append(myList.get(i));
		t2.append(myList.get(i));
		i++;
		}
		System.out.println("t tuple size --> " + t.size());
		System.out.println("t2 tuple size --> " + t2.size());

		//System.out.println(t.toString());
	    inDb.add(t);
	    
	    t2.append("1437586924834.08185029ad71435639949446b9458edb");
	    outDb.add(t2);
	    
	    System.out.println(" after tuple size t2--> " + t2.size());
	    System.out.println(" after tuple size t--> " + t.size());	
	    

	    
	    j++;
	    
		}
		
		input.append(inDb);
	    System.out.println("input="+input.toString());
	    System.out.println("outDb="+outDb.toString()); 

	    
	    //assertEquals(outDb, new Sessionizer().exec(input));
	    assertEquals(outDb, new SessionizerAcc("15m").exec(input));
	}
	
	
	@Test	
	void sessionizerTest_2Tuple_diffSession() throws IOException {
		List<String> myList1 = new ArrayList<String>(Arrays.asList("2015-07-22T17:42:04.834900Z","marketpalce-shop","1.23.0.6","28165","10.0.4.227","80","2.4E-5","0.010813","2.1E-5","200","200","0","254","GET","https://paytm.com:443/papi/v1/promosearch/product/11148776/offers?parent_id=11148811&price=3399&channel=web&version=2","HTTP/1.1","1437586924834"));
		//+ 900005 milli-seconds
		List<String> myList2 = new ArrayList<String>(Arrays.asList("2015-07-22T17:42:04.834900Z","marketpalce-shop","1.23.0.6","28165","10.0.4.227","80","2.4E-5","0.010813","2.1E-5","200","200","0","254","GET","https://paytm.com:443/papi/v1/promosearch/product/11148776/offers?parent_id=11148811&price=3399&channel=web&version=2","HTTP/1.1","1437587824839"));

		TupleFactory mTupleFactory = TupleFactory.getInstance();
		

		
		BagFactory bagFactory = BagFactory.getInstance();
		DataBag inDb = bagFactory.newDefaultBag();
		DataBag outDb = bagFactory.newDefaultBag();
		
		DefaultTuple input = new DefaultTuple();
				
		Tuple t = new DefaultTuple();
		Tuple t2 = new DefaultTuple();


		
		int i=0;
		while (i < 17) {
		t.append(myList1.get(i));
		t2.append(myList1.get(i));
		i++;
		}
		
		System.out.println("t tuple size --> " + t.size());
		System.out.println("t2 tuple size --> " + t2.size());

		//System.out.println(t.toString());
	    inDb.add(t);
	    
	    t2.append("1437586924834.08185029ad71435639949446b9458edb");
	    outDb.add(t2);
	    
	    System.out.println(" after tuple size t2--> " + t2.size());
	    System.out.println(" after tuple size t--> " + t.size());	
	    
// append next tuple:
	    
		Tuple t3 = new DefaultTuple();
		Tuple t4 = new DefaultTuple();
				
		i=0;
		while (i < 17) {
		t3.append(myList2.get(i));
		t4.append(myList2.get(i));
		i++;
		}
		
		System.out.println("t3 tuple size --> " + t3.size());
		System.out.println("t4 tuple size --> " + t4.size());

	    inDb.add(t3);
	    // set the new sessionId: +905 seconds
	    t4.append("1437587824839.08185029ad71435639949446b9458edb");
	    outDb.add(t4);
	    
	    System.out.println(" after tuple size t4--> " + t4.size());
	    System.out.println(" after tuple size t3--> " + t3.size());	
	    
	    
		// confirm final output:
		input.append(inDb);
	    System.out.println("input="+input.toString());
	    System.out.println("outDb="+outDb.toString()); 

	    
	    //assertEquals(outDb, new Sessionizer().exec(input));
	    assertEquals(outDb, new SessionizerAcc("15m").exec(input));
	}

}
