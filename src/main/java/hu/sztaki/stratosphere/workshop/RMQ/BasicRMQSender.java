/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2014 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

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
				channel.basicPublish("", QUEUE_NAME, null, message.getBytes());
				break;
			}
			channel.basicPublish("", QUEUE_NAME, null, message.getBytes());
		}
		channel.close();
		connection.close();
	}
}
