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
 * Tests router dealer pattern. Service acts as router-dealer.
 * Tests check whether all the subscribers (reqs) got their messages and whether they are all live (served equally often)
 * Liveness is likely to fail if threads are not synchronized; it is not a big problem
 */
public class DealerTest
{
    private static final CommManager req1 = new CommManager();
    private static final CommManager req2 = new CommManager();
    private static final CommManager service = new CommManager();
    private static final CommManager rep = new CommManager();

    private static final Random random = new Random(System.currentTimeMillis());
    private static final HashMap<Integer, byte[]> requestOwner = new HashMap<>();
    private static final HashMap<Integer, Integer> requestID = new HashMap<>();
    
    private static final int noEntities = 10;

    private static final HashMap<Integer, Messages.Entity> entitiesReq1 = new HashMap<>();
    private static final HashMap<Integer, Messages.Entity> entitiesReq2 = new HashMap<>();
    
    private static int liveState = -1;
    private static boolean liveCheck = true;
 

    public DealerTest()
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
        service.listenOn("tcp://*:5558");
        service.setRequestHandler(Messages.ReqEntity.getDescriptor(), new MessageHandler() { 
                                public void handleMessage(GeneratedMessage message, byte[] peer_id, int request_id)
                                {
                                        try
                                        {
                                                Thread.sleep(250);
                                        }
                                        catch (Exception ex) { }
                                        Messages.ReqEntity req_entity_message = (Messages.ReqEntity) message;
                                        requestOwner.put(req_entity_message.getId(), peer_id);
                                        requestID.put(req_entity_message.getId(), request_id);
                                        //System.out.println("Forwarding request for "+req_entity_message.getId());
                                        service.request("tcp://*:5559", req_entity_message, Messages.AddEntity.getDescriptor(), new MessageHandler() { 
                                            public void handleMessage(GeneratedMessage message, byte[] peer_id, int request_id)
                                            {
                                                    try
                                                    {
                                                            Thread.sleep(250);
                                                    }
                                                    catch (Exception ex) { }
                                                    Messages.AddEntity add_entity_message = (Messages.AddEntity) message;
                                                    byte[] peer_id_orig = requestOwner.get(add_entity_message.getEntity().getId());
                                                    int request_id_orig = requestID.get(add_entity_message.getEntity().getId());
                                                    service.reply(add_entity_message, peer_id_orig, request_id_orig);
                                                    //System.out.println("Forwarding response for "+add_entity_message.getEntity().getId());
                                             }
                                        });
                                 }
                        });
        rep.listenOn("tcp://*:5559");
        rep.setRequestHandler(Messages.ReqEntity.getDescriptor(), new MessageHandler() { 
                                public void handleMessage(GeneratedMessage message, byte[] peer_id, int request_id)
                                {
                                        try
                                        {
                                                Thread.sleep(250);
                                        }
                                        catch (Exception ex) { }
                                        Messages.ReqEntity req_entity_message = (Messages.ReqEntity) message;
                                        Messages.AddEntity add_entity_message = Messages.AddEntity.newBuilder()
                                                .setEntity(
                                                                Messages.Entity.newBuilder()
                                                                .setId(req_entity_message.getId())
                                                                .setName(createByteString(random, 6))
                                                                .build())
                                                                .build();
                                        rep.reply(add_entity_message, peer_id, request_id);
                                        //System.out.println("Replying with "+add_entity_message.getEntity().getId());
                                }
                            });
        Thread req1Thread = new Thread(req1);
        req1Thread.start();
        Thread req2Thread = new Thread(req2);
        req2Thread.start();
        Thread repThread = new Thread(rep);
        repThread.start();
        Thread serviceThread = new Thread(service);
        serviceThread.start();

        //Requesting
        for(int i=0; i<noEntities; i++)
        {
            if (i%2==0)
            {
                Messages.ReqEntity req_entity = Messages.ReqEntity.newBuilder()
                                                                .setId(i)
                                                                .build();
                req1.request("tcp://*:5558", req_entity, Messages.AddEntity.getDescriptor(), new MessageHandler() { 
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
                                entitiesReq1.put(request_id, add_entity_message.getEntity());
                                //System.out.println("Req1 received entity "+add_entity_message.getEntity().getId()+" request_id: "+request_id);
                        }
                });
            }
            else
            {
                Messages.ReqEntity req_entity = Messages.ReqEntity.newBuilder()
                                                                .setId(i)
                                                                .build();
                req2.request("tcp://*:5558", req_entity, Messages.AddEntity.getDescriptor(), new MessageHandler() { 
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
                                entitiesReq2.put(request_id, add_entity_message.getEntity());
                                //System.out.println("Req2 received entity "+add_entity_message.getEntity().getId()+" request_id: "+request_id);
                        }
                });                
            }   
        }
        
        //closing
        Thread.sleep(20000);
        req1Thread.interrupt();
        req2Thread.interrupt();
        repThread.interrupt();
        serviceThread.interrupt();
        req1Thread.join();
        req2Thread.join();
        repThread.join();
        serviceThread.join();
        req1.close();
        req2.close();
        rep.close();
        service.close();        
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
    
    //Count responses for req1
    @Test
    public void countResponsesReq1()
    {
        //even entities
        boolean check = true;
        for (int i=0; i<noEntities/2;i++)
        {
            Messages.Entity e = entitiesReq1.get(i+1);
            if (e.getId() != i*2)
            {
                check = false;
                break;
            }
        }
        assertTrue("Req1 didnt get all the right entities: "+entitiesReq1.size()+" of "+noEntities/2,check);
    }
    
    //Count responses for req2
    @Test
    public void countResponsesReq2()
    {
        //odd entities
        boolean check = true;
        for (int i=0; i<noEntities/2;i++)
        {
            Messages.Entity e = entitiesReq2.get(i+1);
            if (e.getId() != i*2+1)
            {
                check = false;
                break;
            }
        }
        assertTrue("Req2 didnt get all the right entities: "+entitiesReq2.size()+" of "+noEntities/2,check);
    }

    //testing liveness
    @Test
    public void testLiveness()
    {
        assertTrue("Clients are not served equally often.",liveCheck);        
    }    
}
