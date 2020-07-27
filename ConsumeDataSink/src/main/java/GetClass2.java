import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.commons.lang3.time.StopWatch;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.AMQP.Queue.DeclareOk;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

public class GetClass2 {
	//private Set<Integer> SeenMessage = new HashSet<Integer>();

	
	public static void main(String[] args) throws IOException, TimeoutException {
		// TODO Auto-generated method stub
		ConnectionFactory factory = new ConnectionFactory();
		factory.setAutomaticRecoveryEnabled(false);
		  factory.setHost("localhost");
		  factory.setUsername("guest");
		  factory.setPassword("guest");
		  com.rabbitmq.client.Connection connection = factory.newConnection();
		  
		 // StopWatch watch = new StopWatch();
		 
		 // List<String> myMessages = new ArrayList<String>();
		  //channel.basicQos(1000);
		 
final Runnable producer = () -> {
		  try {
			  Channel channel = connection.createChannel();
			  channel.queueDeclare("Singleton_claims_processed_Queue",true, false, false, null);
			  GenerateMessageBlock messageBlock = new GenerateMessageBlock();
				 messageBlock.start();
			channel.basicConsume("Singleton_claims_processed_Queue",true, new DefaultConsumer(channel) {
			 
				  
				  
				  @Override
				     public void handleDelivery(
				        String consumerTag,
				        Envelope envelope, 
				        AMQP.BasicProperties properties, 
				        byte[] body) throws IOException {
					  
					 
				            		messageBlock.addData(new String(body, "UTF-8")+ System.lineSeparator());
				            	
				            		 //channel.basicAck(envelope.getDeliveryTag(), true); // new channel with auto ack will be created. then this can be removed
								
							} 
				            // insert the message
				            
				            
				            
			                
			               
				     });
				                
				            
				            
				            
				 
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
	
			
	
	};

new Thread(producer).start();





}
}
