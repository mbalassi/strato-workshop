package hu.sztaki.stratosphere.workshop.RMQ;

import java.io.BufferedReader;
import java.io.InputStreamReader;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

public class BasicRMQSender {
	private final static String QUEUE_NAME = "hello";

	public static void main(String[] argv) throws java.io.IOException {
		
		ConnectionFactory factory = new ConnectionFactory();
		factory.setHost("localhost");
		Connection connection = factory.newConnection();
		Channel channel = connection.createChannel();
		channel.queueDeclare(QUEUE_NAME, false, false, false, null);
		
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
		
		String message;
		
		while(true){
			System.out.println("Enter next message (q to quit):");
			message = br.readLine();
			if(message.equals("q")){
				break;
			}
			channel.basicPublish("", QUEUE_NAME, null, message.getBytes());
		}
		channel.close();
		connection.close();
	}
}
