import java.io.IOException;
import java.net.InetAddress;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Scanner;

import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.QueueingConsumer;


public class Test {
	
	public static String succIp;
	public static int succHash;
	public static String predIp = null;
	public static int predHash;
	
	public static String myIp;
	public static int myHash;
	
	public static String fingers [] = new String[Utils.RING_LEN];
	
	public static void main(String [] args) throws Exception{
		InetAddress IP = InetAddress.getLocalHost();
		myIp = IP.getHostAddress();
		
		myIp = "111.111.111.111";
		
		myHash = Utils.hash(myIp);
		System.out.printf("My IP and Hash: %s %d\n", myIp, myHash);
		
		succIp = myIp;
		succHash = myHash;
		
		Scanner in = new Scanner(System.in);
		String existingNodeIp = "";
		
		System.out.print("Please enter existing IP in the system: ");
		
		while(true){
			String line = in.nextLine();
			if (Utils.isValidIp(line)) {
				existingNodeIp = line;
				break;
			} else {
				System.out.print("You've entered invalid ip! Please try again: ");
			}
		}

		succIp = findSuccessor(existingNodeIp, myIp, myHash);
		succHash = Utils.hash(succIp);
		updateSuccessor();
		System.out.printf("Successor IP and Hash: %s %d\n", succIp, succHash);

		ReceivingThread receivingThread = new ReceivingThread(myIp);
		receivingThread.start();

		invokeNotifiedThread();
		sendNotification(succIp, myIp, myHash);
		
		buildFingers();
		showFingers();
		
		invokeStableThread();
		invokeFixingFingersThread();

		while(true){
			int k = in.nextInt();
			if ( k == 0 ) break;
		}
		
	}
	
	private static void updateSuccessor() {
		// TODO Auto-generated method stub
		int mod = (1<<Utils.RING_LEN);
		for (int i=0;i<Utils.RING_LEN;i++){
			int targetKey = (myHash + (1<<i)) % mod;
			if (Utils.isBetween(targetKey, myHash, succHash)){
				fingers[i] = succIp;
			}
			else break;
		}
	}

	private static void showFingers() {
		// TODO Auto-generated method stub
		int mod = (1<<Utils.RING_LEN);
		for (int i=0;i<Utils.RING_LEN;i++){
			int targetKey = (myHash + (1<<i)) % mod;
			//System.out.println("target key and ip: " + targetKey + " " + fingers[i]);
		}
	}

	private static void buildFingers() throws Exception {
		int mod = (1<<Utils.RING_LEN);
		int start = 0;
		while(start < Utils.RING_LEN){
			int targetKey = (myHash + (1<<start)) % mod;
			if (myIp.equals(succIp) || Utils.isBetween(targetKey, myHash, succHash)){
				fingers[start++] = succIp;
			}
			else break;
		}
		for (int i=start;i<Utils.RING_LEN;i++){
			fingers[i] = findSuccessor(succIp, myIp, (myHash + (1<<i)) % mod);
		}
	}


	private static void invokeFixingFingersThread() {
		Thread fixingFingersThread = new Thread(new Runnable() {
			
			@Override
			public void run() {
				try {
					int mod = (1<<Utils.RING_LEN);
					int start = 0;
					while(start < Utils.RING_LEN){
						int targetKey = (myHash + (1<<start)) % mod;
						if (myIp.equals(succIp) || Utils.isBetween(targetKey, myHash, succHash)){
							fingers[start++] = succIp;
						}
						else break;
					}
					int cur = start;
					//System.out.println("start = " + start);
					while(true){
						if (cur == Utils.RING_LEN ){
							start = 0;
							while(start < Utils.RING_LEN){
								int targetKey = (myHash + (1<<start)) % mod;
								if (myIp.equals(succIp) || Utils.isBetween(targetKey, myHash, succHash)){
									fingers[start++] = succIp;
								}
								else break;
							}
							cur = start;
						}
						if (cur<Utils.RING_LEN){
							//System.out.println(cur);
							fingers[cur] = findSuccessor(fingers[cur-1], myIp, (myHash + (1<<cur)) % mod);
							cur++;
						}
						Thread.sleep(Utils.FINGER_TIMER);
					}
				}catch(Exception e){
					e.printStackTrace();
				}
			}
		});
		fixingFingersThread.start();
	}
	
	private static void invokeNotifiedThread() {
		Thread notifiedThread = new Thread(new Runnable() {
			
			@Override
			public void run() {
				try {
					Connection connection = ConnectionStorage.getConnection(myIp).connection;
				    Channel channel = connection.createChannel();
				    channel.queueDeclare(Utils.NOTIFY_QUEUE + myIp, false, false, false, null);
				    QueueingConsumer consumer = new QueueingConsumer(channel);
				    channel.basicConsume(Utils.NOTIFY_QUEUE + myIp, true, consumer);
				    while (true) {
				      QueueingConsumer.Delivery delivery = consumer.nextDelivery();
				      String message = new String(delivery.getBody());
				      NotifyRequest req = NotifyRequest.parse(message);
				      if (req == null){
				    	  System.out.println("Can't parse NotifyRequest!");
				    	  continue;
				      }
				      processNotifyMessage(req);
				    }
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}

			private void processNotifyMessage(NotifyRequest req) {
				//System.out.println("1) " + req);
				// TODO Auto-generated method stub
				if (predIp == null || predIp.equals(myIp) || Utils.isBetween(req.senderKey, predHash, myHash)){
					String temp = predIp;
					predIp = req.senderIp;
					predHash = Utils.hash(predIp);
					if (!req.senderIp.equals(temp)){
						System.out.println("My Predecessor Ip and Hash: " + predIp + " , " + predHash);						
					}
				}
			}
		});
		notifiedThread.start();
	}


	private static void invokeStableThread() {
		Thread stableThread = new Thread(new Runnable() {
			
			@Override
			public void run() {
				// TODO Auto-generated method stub
				try {
					while(true){
						sendFindRequest(succIp, FindRequest.FIND_PREDECESSOR, myIp, myHash);
						String succPredIp = getFoundSuccessor(FoundRequest.FOUND_PREDECESSOR);
						int succPredHash = Utils.hash(succPredIp);
						if (!Utils.isBetween(myHash, succPredHash, succHash)){
							String temp = succIp;
							succIp = succPredIp;
							succHash = succPredHash;
							updateSuccessor();
							if(!succPredIp.equals(temp))
								System.out.println("My Successor Ip and Hash: " + succIp + " , " + succHash);
						}
						sendNotification(succIp, myIp, myHash);
						Thread.sleep(Utils.STABLE_TIMER);
					}
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		});
		stableThread.start();
	}
	private static void sendNotification(String toIp, String myIp, int myHash) throws Exception {
		// TODO Auto-generated method stub
		Connection connection = ConnectionStorage.getConnection(toIp).connection;
	    Channel channel = connection.createChannel();
	    channel.queueDeclare(Utils.NOTIFY_QUEUE + toIp, false, false, false, null);
	    NotifyRequest req = new NotifyRequest(myIp, myHash);
	    String message = req.toString();
	    //System.out.println("2) " + req);
	    channel.basicPublish("", Utils.NOTIFY_QUEUE + toIp, null, message.getBytes());
	    channel.close();
	}

	private static String getFoundSuccessor(String type) throws Exception{
		Connection connection = ConnectionStorage.getConnection(myIp).connection;
	    Channel channel = connection.createChannel();
	    channel.queueDeclare(Utils.FOUND_QUEUE + myIp, false, false, false, null);
	    QueueingConsumer consumer = new QueueingConsumer(channel);
	    channel.basicConsume(Utils.FOUND_QUEUE + myIp, true, consumer);
	    while (true) {
	    	//System.out.println("in the while");
	      QueueingConsumer.Delivery delivery = consumer.nextDelivery();
	      String message = new String(delivery.getBody());
	      FoundRequest req = FoundRequest.parse(message);
	      if ( req == null ){
	    	  System.out.println("Can't parse FoundRequest!");
	    	  continue;
	      }
	      //System.out.println(req);
	      if(req.type.equals(type)){
	    	  channel.close();
	    	  return req.ip;
	      }
	    }
	}
	
	private static void sendFindRequest(String toIp, String type, String senderIp, int key) throws Exception{
		Connection connection = ConnectionStorage.getConnection(toIp).connection;
	    Channel channel = connection.createChannel();
	    channel.queueDeclare(Utils.FIND_QUEUE + toIp, false, false, false, null);
	    FindRequest req = new FindRequest(type, senderIp, key);
	    String message = req.toString();
	    //System.out.println("3) " + req);
	    channel.basicPublish("", Utils.FIND_QUEUE + toIp, null, message.getBytes());
	    channel.close();
	}
	
	private static String findSuccessor(String toIp, String senderIp, int key) throws Exception {
		//System.out.println("Kirdi");
		if (toIp.equals(senderIp)) return senderIp;
		sendFindRequest(toIp, FindRequest.FIND_SUCCESSOR, senderIp, key);
	    return getFoundSuccessor(FoundRequest.FOUND_SUCCESSOR);
	}
	
	public static void processfindMessage(FindRequest req) throws Exception{
		//System.out.println("4) " + req.toString());
		if(req.type.equals(FindRequest.FIND_SUCCESSOR)){
			//System.out.println("4) " + req.toString());
			if (myIp.equals(succIp) || Utils.isBetween(req.key, myHash, succHash)){
				sendFoundRequest(req.requesterIp, FoundRequest.FOUND_SUCCESSOR, succIp, req.key);
				if (myIp.equals(succIp)){
					succIp = req.requesterIp;
					succHash = Utils.hash(succIp);
					updateSuccessor();
					sendNotification(succIp, myIp, myHash);
					System.out.println("xx My Successor IP and Hash: " + succIp + " , " + succHash);
				}
				return;
			}
			for (int i=Utils.RING_LEN-1;i>=0;i--){
				int curKey = Utils.hash(fingers[i]);
				if (myIp.equals(fingers[i]) || Utils.isBetween(req.key, myHash, curKey)) continue;
				sendFindRequest(fingers[i], FindRequest.FIND_SUCCESSOR, req.requesterIp, req.key);
				return;
			}
			sendFoundRequest(req.requesterIp, FoundRequest.FOUND_SUCCESSOR, myIp, req.key);
		}
		else {
			String curPredIp = predIp;
			if (predIp == null){
				curPredIp = succIp;
			}
			sendFoundRequest(req.requesterIp, FoundRequest.FOUND_PREDECESSOR, curPredIp, req.key);
		}
	}

	private static void sendFoundRequest(String toIp, String type, String ip, int key) throws Exception {
		Connection connection = ConnectionStorage.getConnection(toIp).connection;
	    Channel channel = connection.createChannel();
	    channel.queueDeclare(Utils.FOUND_QUEUE + toIp, false, false, false, null);
	    FoundRequest req = new FoundRequest(type, ip, key);
	    String message = req.toString();
	    //System.out.println("5) " + req);
	    channel.basicPublish("", Utils.FOUND_QUEUE + toIp, null, message.getBytes());
	    channel.close();
	}
}


class ReceivingThread extends Thread{
	public String listenIp;
	
	public ReceivingThread(String ip) {
		// TODO Auto-generated constructor stub
		listenIp = ip;
	}
	
	@Override
	public void run() {
		try {
			Connection connection = ConnectionStorage.getConnection(listenIp).connection;
		    Channel channel = connection.createChannel();
		    channel.queueDeclare(Utils.FIND_QUEUE + listenIp, false, false, false, null);
		    QueueingConsumer consumer = new QueueingConsumer(channel);
		    channel.basicConsume(Utils.FIND_QUEUE + listenIp, true, consumer);
		    while (true) {
		      QueueingConsumer.Delivery delivery = consumer.nextDelivery();
		      String message = new String(delivery.getBody());
		      FindRequest req = FindRequest.parse(message);
		      if (req == null){
		    	  System.out.println("Can't parse FindRequest!");
		    	  continue;
		      }
		      Test.processfindMessage(req);
		    }
		}catch (Exception e){
			e.printStackTrace();
		}
	}
}





