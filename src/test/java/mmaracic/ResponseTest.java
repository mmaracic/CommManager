/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package test.java.mmaracic;

import com.google.protobuf.ByteString;
import com.google.protobuf.GeneratedMessage;
import com.wispear.comm.CommManager;
import com.wispear.comm.MessageHandler;
import com.wispear.comm.Messages;
import java.util.HashMap;
import java.util.Random;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author Marijo
 * This test tests whether all the entities have been added and all requests responded.
 */
public class ResponseTest
{
    private static final CommManager client1 = new CommManager();
    private static final CommManager client2 = new CommManager();
    private static final CommManager service = new CommManager();
    private final static Random random = new Random(System.currentTimeMillis());
    private static final HashMap<Integer, Messages.Entity> entities = new HashMap<>();
    private static final int noEntities = 10;
    private static int noResponses = 0;

    public ResponseTest()
    {
    }
    
    private static ByteString createByteString(Random random, int length)
    {
            byte[] buffer = new byte[length];
            random.nextBytes(buffer);
            return ByteString.copyFrom(buffer);
    }
    
    @BeforeClass
    public static void setUpClass() throws InterruptedException
    {
        // Bind the entity management request listener
        service.listenOn("tcp://*:5556");
        // Set handler for the Request Entity message (i.e., the message where a client asks for entity information)
        service.setRequestHandler(Messages.ReqEntity.getDescriptor(), 
                        new MessageHandler() { 
                                public void handleMessage(GeneratedMessage message, byte[] peer_id, int request_id)
                                {
                                        try
                                        {
                                                Thread.sleep(500);
                                        }
                                        catch (Exception ex) { }

                                        Messages.ReqEntity req_entity_message = (Messages.ReqEntity) message;

                                        Messages.Entity entity;
                                        // Check if a entity with this id already exists
                                        if (entities.containsKey(req_entity_message.getId()))
                                        {
                                                entity = entities.get(req_entity_message.getId());
                                                //System.out.println("Service found entity "+req_entity_message.getId());
                                        }
                                        else
                                        {
                                                entity = Messages.Entity.newBuilder()
                                                                .setId(0xFFFFFFFF)
                                                                .setName(createByteString(random, 6))
                                                                .build();
                                                //System.out.println("Service didnt find entity "+req_entity_message.getId());
                                        }
                                        service.reply(entity, peer_id, request_id);
                                }
                        });
        // Set handler for Add Entity message, when a client asks to register new entity
        service.setRequestHandler(Messages.AddEntity.getDescriptor(),
                        new MessageHandler() { 
                                public void handleMessage(GeneratedMessage message, byte[] peer_id, int request_id)
                                {
                                        try
                                        {
                                                Thread.sleep(250);
                                        }
                                        catch (Exception ex) { }

                                        Messages.AddEntity add_entity_message = (Messages.AddEntity) message;
                                        Messages.Entity entity = add_entity_message.getEntity();
                                        entities.put(entity.getId(), entity);
                                        //System.out.println("Service added entity " + entity.getId());
                                }
                        });

        // After having set the callbacks, start running

        Thread clientThread1 = new Thread(client1);
        clientThread1.start();

        Thread clientThread2 = new Thread(client2);
        clientThread2.start();

        Thread serviceThread = new Thread(service);
        serviceThread.start();

        for (int i = 0; i < noEntities; i++)
        {
                // Ask for entity info
                Messages.ReqEntity req_entity = Messages.ReqEntity.newBuilder()
                                                                .setId(i)
                                                                .build();
                //System.out.println("Cient1 asking for entity " + i);
                client1.request("tcp://localhost:5556", req_entity, Messages.Entity.getDescriptor(),
                                new MessageHandler() 
                                        {
                                                // Check the response - it is either the details of the requested entity, or an id which means there is no such registered entity
                                                public void handleMessage(GeneratedMessage message, byte[] peer_id, int request_id)
                                                {
                                                        Messages.Entity entity_message = (Messages.Entity) message;
                                                        noResponses++;
                                                        if (entity_message.getId() != 0xFFFFFFFF)
                                                        {
//									System.out.println(message.toString());
                                                        }
                                                        else
                                                        {
//									System.out.println("No entity with this id");
                                                        }
                                                }
                                        }
                                );

                // Create new entity
                Messages.AddEntity add_entity = Messages.AddEntity.newBuilder()
                                                                .setEntity(
                                                                                Messages.Entity.newBuilder()
                                                                                .setId(9 - i)
                                                                                .setName(createByteString(random, 6))
                                                                                .build())
                                                                                .build();
                client2.request("tcp://localhost:5556", add_entity, null, null); // No need for callback, as there is no reply
                //System.out.println("Client2 adding entity"+add_entity.getEntity().getId());
        }

        // Close
        Thread.sleep(10000);
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
            
    @AfterClass
    public static void tearDownClass()
    {
    }
    
    @Before
    public void setUp()
    {
    }
    
    @After
    public void tearDown()
    {
    }

    @Test
    public void countEntities()
    {
        assertTrue("Not all entities have been added: "+entities.size()+" of "+noEntities, entities.size()==noEntities);
    }

    @Test
    public void countResponses()
    {
        assertTrue("Not all responses have been received: "+noResponses+" of "+noEntities, noResponses==noEntities);
    }
}
