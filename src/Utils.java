
public class Utils {

	public static final int RING_LEN = 4;
	public static final String FIND_QUEUE = "FIND";
	public static final String FOUND_QUEUE = "FOUND";
	public static final String NOTIFY_QUEUE = "NOTIFY_QUEUE";
	protected static final long STABLE_TIMER = 1000;
	protected static final long FINGER_TIMER = 1000;

	public static int hash(String ip) {
		int mod = (1<<RING_LEN);
		return (ip.hashCode()%mod + mod)%mod;
	}

	public static boolean isValidIp(String ip) {
		final String PATTERN = "^(([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\.){3}([01]?\\d\\d?|2[0-4]\\d|25[0-5])$";
		return ip != null && ip.matches(PATTERN);
	}

	public static boolean isBetween(int key, int myHash, int succHash) {
		if (succHash >= myHash && myHash <= key && key <= succHash) return true;
		if (succHash < myHash && !(succHash < key && key < myHash) ) return true;
		return false;
	}
	
}
