package pigUDFTest;

public class Test {

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
	
	public  static  void main (String [] args) {	
		
		Test t = new Test();
		String md5 = "1.23.0.6";
		String ts = "1437586924834";
		String md5_ret = t.MD5(md5);
		String result = ts + ".".concat(md5_ret);
		System.out.println(result);
	}
}
