
public class NotifyRequest {
	public String senderIp;
	public int senderKey;
	
	public NotifyRequest(String senderIp, int senderKey) {
		this.senderIp = senderIp;
		this.senderKey = senderKey;
	}
	
	@Override
	public String toString() {
		return "SENDER_IP:" + senderIp + ",SENDER_KEY:" + senderKey;
	}
	
	public static NotifyRequest parse(String data) {
		try {
			String a[] = data.split(",");
			String v0[] = a[0].split(":");
			String v1[] = a[1].split(":");
			NotifyRequest r = new NotifyRequest(v0[1], Integer.parseInt(v1[1]));
			return r;
		} catch (Exception e) {
			return null;
		}
	}
	
	
}
