
import java.util.*;

import com.google.protobuf.ByteString;
import com.google.protobuf.GeneratedMessage;
import com.wispear.comm.*;
import com.wispear.comm.Messages.*;



public class Test {
	
	private static Hashtable<Integer, Entity> entities;

	public static void main(String[] args) throws InterruptedException {
		// TODO Auto-generated method stub
		
		
		/*
		 * This application has three CommManager objects, running on separate threads. They are in the same process, but obviously this is
		 * equivelent to different processes running on different machines.
		 * There is one object for a Service, and two for Client (simulating different clients).
		 * One of the clients sends the Service new entity messages, and the other requests entity infromation from the entity management.
		 * Both of them do it simulatenously.
		 */
		final CommManager client1 = new CommManager();
		final CommManager client2 = new CommManager();
		final CommManager service = new CommManager();
		final Random random = new Random(System.currentTimeMillis());
		entities = new Hashtable<Integer, Entity>();
		
		// Bind the entity management request listener
		service.listenOn("tcp://*:5556");
		// Set handler for the Request Entity message (i.e., the message where a client asks for entity information)
		service.setRequestHandler(ReqEntity.getDescriptor(), 
				new MessageHandler() { 
					public void handleMessage(GeneratedMessage message, byte[] peer_id, int request_id)
					{
						try
						{
							Thread.sleep(500);
						}
						catch (Exception ex) { }
						
						ReqEntity req_entity_message = (ReqEntity) message;
						
						Entity entity;
						// Check if a entity with this id already exists
						if (entities.containsKey(req_entity_message.getId()))
						{
							entity = entities.get(req_entity_message.getId());
						}
						else
						{
							entity = Entity.newBuilder()
									.setId(0xFFFFFFFF)
									.setName(createByteString(random, 6))
									.build();
						}
						service.reply(entity, peer_id, request_id);
					}
				});
		// Set handler for Add Entity message, when a client asks to register new entity
		service.setRequestHandler(AddEntity.getDescriptor(),
				new MessageHandler() { 
					public void handleMessage(GeneratedMessage message, byte[] peer_id, int request_id)
					{
						try
						{
							Thread.sleep(250);
						}
						catch (Exception ex) { }
						
						AddEntity add_entity_message = (AddEntity) message;
						Entity entity = add_entity_message.getEntity();
						entities.put(entity.getId(), entity);
						System.out.println("Entity added " + entity.getId());
					}
				});
		
		// After having set the callbacks, start running
		
		Thread clientThread1 = new Thread(client1);
		clientThread1.start();
		
		Thread clientThread2 = new Thread(client2);
		clientThread2.start();
		
		Thread serviceThread = new Thread(service);
		serviceThread.start();
		
		for (int i = 0; i < 10; i++)
		{
			// Ask for entity info
			ReqEntity req_entity = ReqEntity.newBuilder()
									.setId(i)
									.build();
			System.out.println("Ask entity " + i);
			client1.request("tcp://localhost:5556", req_entity, Entity.getDescriptor(),
					new MessageHandler() 
						{
							// Check the response - it is either the details of the requested entity, or an id which means there is no such registered entity
							public void handleMessage(GeneratedMessage message, byte[] peer_id, int request_id)
							{
								Entity entity_message = (Entity) message;
								if (entity_message.getId() != 0xFFFFFFFF)
								{
									System.out.println(message.toString());
								}
								else
								{
									System.out.println("No entity with this id");
								}
							}
						}
					);
			
			// Create new entity
			AddEntity add_entity = AddEntity.newBuilder()
									.setEntity(
											Entity.newBuilder()
											.setId(9 - i)
											.setName(createByteString(random, 6))
											.build())
											.build();
			client2.request("tcp://localhost:5556", add_entity, null, null); // No need for callback, as there is no reply
		}
		
		// Close
		Thread.sleep(20000);
		clientThread1.interrupt();
		clientThread2.interrupt();
		serviceThread.interrupt();
		clientThread1.join();
		clientThread2.join();
		serviceThread.join();
		service.close();
		client1.close();
		client2.close();
	}
	
	private static ByteString createByteString(Random random, int length)
	{
		byte[] buffer = new byte[length];
		random.nextBytes(buffer);
		return ByteString.copyFrom(buffer);
	}
}
