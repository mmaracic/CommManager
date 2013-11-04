
import java.util.*;

import com.google.protobuf.ByteString;
import com.google.protobuf.GeneratedMessage;
import com.wispear.wisespear.comm.*;
import com.wispear.wisespear.comm.Messages.AddDevice;
import com.wispear.wisespear.comm.Messages.Device;
import com.wispear.wisespear.comm.Messages.ReqDevice;



public class Test {
	
	private static Hashtable<Integer, Device> devices;

	public static void main(String[] args) throws InterruptedException {
		// TODO Auto-generated method stub
		
		
		/*
		 * This application has three CommManager objects, running on separate threads. They are in the same process, but obviously this is
		 * equivelent to different processes running on different machines.
		 * There is one object for Entity Management, and two for SpearInt (simulating different clients).
		 * One of the clients sends the Entity Management new device messages, and the other requests device infromation from the entity management.
		 * Both of them do it simulatenously.
		 */
		final CommManager spearint1 = new CommManager();
		final CommManager spearint2 = new CommManager();
		final CommManager entity = new CommManager();
		final Random random = new Random(System.currentTimeMillis());
		devices = new Hashtable<Integer, Device>();
		
		// Bind the entity management request listener
		entity.listenOn("tcp://*:5556");
		// Set handler for the Request Device message (i.e., the message where a client asks for device information)
		entity.setRequestHandler(ReqDevice.getDescriptor(), 
				new MessageHandler() { 
					public void handleMessage(GeneratedMessage message, byte[] peer_id, int request_id)
					{
						try
						{
							Thread.sleep(500);
						}
						catch (Exception ex) { }
						
						ReqDevice req_device_message = (ReqDevice) message;
						
						Device device;
						// Check if a device with this id already exists
						if (devices.containsKey(req_device_message.getId()))
						{
							device = devices.get(req_device_message.getId());
						}
						else
						{
							device = Device.newBuilder()
									.setId(0xFFFFFFFF)
									.setMacAddress(createByteString(random, 6))
									.build();
						}
						entity.reply(device, peer_id, request_id);
					}
				});
		// Set handler for Add Device message, when a client asks to register new device
		entity.setRequestHandler(AddDevice.getDescriptor(),
				new MessageHandler() { 
					public void handleMessage(GeneratedMessage message, byte[] peer_id, int request_id)
					{
						try
						{
							Thread.sleep(250);
						}
						catch (Exception ex) { }
						
						AddDevice add_device_message = (AddDevice) message;
						Device device = add_device_message.getDevice();
						devices.put(device.getId(), device);
						System.out.println("Device added " + device.getId());
					}
				});
		
		// After having set the callbacks, start running
		
		Thread spearintThread1 = new Thread(spearint1);
		spearintThread1.start();
		
		Thread spearintThread2 = new Thread(spearint2);
		spearintThread2.start();
		
		Thread entityThread = new Thread(entity);
		entityThread.start();
		
		for (int i = 0; i < 10; i++)
		{
			// Ask for device info
			ReqDevice req_device = ReqDevice.newBuilder()
									.setId(i)
									.build();
			System.out.println("Ask device " + i);
			spearint1.request("tcp://localhost:5556", req_device, Device.getDescriptor(),
					new MessageHandler() 
						{
							// Check the response - it is either the details of the requested device, or an id which means there is no such registered device
							public void handleMessage(GeneratedMessage message, byte[] peer_id, int request_id)
							{
								Device device_message = (Device) message;
								if (device_message.getId() != 0xFFFFFFFF)
								{
									System.out.println(message.toString());
								}
								else
								{
									System.out.println("No device with this id");
								}
							}
						}
					);
			
			// Create new device
			AddDevice add_device = AddDevice.newBuilder()
									.setDevice(
											Device.newBuilder()
											.setId(9 - i)
											.setMacAddress(createByteString(random, 6))
											.build())
											.build();
			spearint2.request("tcp://localhost:5556", add_device, null, null); // No need for callback, as there is no reply
		}
		
		// Close
		Thread.sleep(20000);
		spearintThread1.interrupt();
		spearintThread2.interrupt();
		entityThread.interrupt();
		spearintThread1.join();
		spearintThread2.join();
		entityThread.join();
		entity.close();
		spearint1.close();
		spearint2.close();
	}
	
	private static ByteString createByteString(Random random, int length)
	{
		byte[] buffer = new byte[length];
		random.nextBytes(buffer);
		return ByteString.copyFrom(buffer);
	}
}
