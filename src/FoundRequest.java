public class FoundRequest {
	
	public String ip;
	public int key;
	public String type;
	public static String FOUND_SUCCESSOR = "FOUND_SUCCESSOR";
	public static String FOUND_PREDECESSOR = "FOUND_PREDECESSOR";
	
	public FoundRequest(String type, String ip, int key) {
		this.type = type;
		this.key = key;
		this.ip = ip;
	}
	
	@Override
	public String toString() {
		return "TYPE:" + type + ",IP:" + ip + ",REQESTED_KEY:" + key;
	}
	
	public static FoundRequest parse(String data) {
		try {
			String a[] = data.split(",");
			String v0[] = a[0].split(":");
			String v1[] = a[1].split(":");
			String v2[] = a[2].split(":");
			FoundRequest r = new FoundRequest(v0[1], v1[1], Integer.parseInt(v2[1]));
			return r;
		} catch (Exception e) {
			return null;
		}
	}
	
}
