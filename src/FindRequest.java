public class FindRequest {
	
	public String requesterIp;
	public int key;
	public String type;
	public static String FIND_SUCCESSOR = "FIND_SUCCESSOR";
	public static String FIND_PREDECESSOR = "FIND_PREDECESSOR";
	
	public FindRequest(String type, String ip, int key) {
		this.type = type;
		this.key = key;
		this.requesterIp = ip;
	}
	
	@Override
	public String toString() {
		return "TYPE:" + type + ",REQUESTOR_IP:" + requesterIp + ",SUCCESSOR_KEY:" + key;
	}
	
	public static FindRequest parse(String data) {
		try {
			String a[] = data.split(",");
			String v0[] = a[0].split(":");
			String v1[] = a[1].split(":");
			String v2[] = a[2].split(":");
			FindRequest r = new FindRequest(v0[1], v1[1], Integer.parseInt(v2[1]));
			return r;
		} catch (Exception e) {
			return null;
		}
	}
	
	
	
}
