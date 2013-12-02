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
 * Service is a publisher that publishes entities to the subscribers
 * Tests check whether all the subscribers got their messages and whether they are all live (served equally often)
 * Liveness is likely to fail if threads are not synchronized; it is not a big problem
 * Publisher cant have own process thread due to lack of thread safety in zeromq socket
 * We need to allow the subscribers to setup properly because publisher will drop messages it cannot deliver
 */
public class PublisherTest
{
    private static final CommManager client1 = new CommManager();
    private static final CommManager client2 = new CommManager();
    private static final CommManager service = new CommManager();

    private static final Random random = new Random(System.currentTimeMillis());
    private static final HashMap<Integer, Messages.Entity> clientEntities1 = new HashMap<>();
    private static final HashMap<Integer, Messages.Entity> clientEntities2 = new HashMap<>();
    
    private static final int noEntities = 10;
    
    private static int liveState = -1;
    private static boolean liveCheck = true;
    
    public PublisherTest()
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
         service.bindPublisher("tcp://*:5557");
        //Adding handler to the client1 for the AddEntity messages sent by the service
        client1.subscribeTo("tcp://*:5557",Messages.AddEntity.getDescriptor(),
                        new MessageHandler() { 
                                public void handleMessage(GeneratedMessage message, byte[] peer_id, int request_id)
                                {
                                        try
                                        {
                                                Thread.sleep(250);
                                        }
                                        catch (Exception ex) { }
                                        if (liveState==0)
                                            liveCheck = false;
                                        else
                                            liveState=0;

                                        Messages.AddEntity add_entity_message = (Messages.AddEntity) message;
                                        Messages.Entity entity = add_entity_message.getEntity();
                                        clientEntities1.put(entity.getId(), entity);
                                        //System.out.println("Client1 added entity " + entity.getId());
                                }
                        });
        //Adding handler to the client2 for the AddEntity messages sent by the service
        client2.subscribeTo("tcp://*:5557",Messages.AddEntity.getDescriptor(),
                        new MessageHandler() { 
                                public void handleMessage(GeneratedMessage message, byte[] peer_id, int request_id)
                                {
                                        try
                                        {
                                                Thread.sleep(250);
                                        }
                                        catch (Exception ex) { }
                                        if (liveState==1)
                                            liveCheck = false;
                                        else
                                            liveState=1;

                                        Messages.AddEntity add_entity_message = (Messages.AddEntity) message;
                                        Messages.Entity entity = add_entity_message.getEntity();
                                        clientEntities2.put(entity.getId(), entity);
                                        //System.out.println("Client2 added entity " + entity.getId());
                                }
                        });
        Thread clientThread1 = new Thread(client1);
        clientThread1.start();

        Thread clientThread2 = new Thread(client2);
        clientThread2.start();
        
        //Allow the subscribers to setup because publisher will drop messages it cannot deliver
        Thread.sleep(1000);

//        Thread serviceThread = new Thread(service);
//        serviceThread.start();

        //Publishing entities
        //System.out.println("Publishing...");
        for(int i=0; i<noEntities; i++)
        {
            Messages.AddEntity addEntity = Messages.AddEntity.newBuilder()
                                                            .setEntity(
                                                                            Messages.Entity.newBuilder()
                                                                            .setId(i)
                                                                            .setName(createByteString(random, 6))
                                                                            .build())
                                                                            .build();
            service.publish(addEntity);
            //System.out.println("Published entity no: "+i);
        }
        // Close

        Thread.sleep(10000);
        clientThread1.interrupt();
        clientThread2.interrupt();
//        serviceThread.interrupt();
        clientThread1.join();
        clientThread2.join();
//        serviceThread.join();
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

    //Testing first client
    @Test
    public void testFirstClient()
    {
        boolean check = true;
        for(int i=0; i<noEntities; i++)
        {
            if (!clientEntities1.containsKey(i))
            {
                check = false;
                break;
            }
        }
        assertTrue("Client1 did not receive all the entities: "+clientEntities1.size()+" of "+noEntities,check);
    }

    //Testing second client
    @Test
    public void testSecondClient()
    {
       boolean check = true;
        for(int i=0; i<noEntities; i++)
        {
            if (!clientEntities2.containsKey(i))
            {
                check = false;
                break;
            }
        }
        assertTrue("Client2 did not receive all the entities: "+clientEntities2.size()+" of "+noEntities,check);
    }
    
    //testing liveness
    @Test
    public void testLiveness()
    {
        assertTrue("Clients are not served equally often.",liveCheck);        
    }
    
}
