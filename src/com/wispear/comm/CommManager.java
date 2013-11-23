package com.wispear.comm;
import java.lang.reflect.*;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.PriorityBlockingQueue;

import org.zeromq.ZMQ;

import com.google.protobuf.Descriptors.*;
import com.google.protobuf.GeneratedMessage;



/**
 * The CommManager is a library which provides a Protocol Buffer based communication layer in several different patterns.
 * It is based on ZeroMQ, but can be changed transparently to its users.
 * The CommManager has 4 types of sockets: A publisher meant to publish data to other (unknown) services, a subscriber meant to receive information 
 * from services, a router to receive requests (which require a reply), and a variable amount of dealers to perform requests (which will be answered
 * with a reply).
 * All of the incoming communication is monitored by a ZMQ poller, which distributes it to the appropriate socket, and runs the correct callback function
 * (implementation of the MessageHandler interface).
 * On the outgoing side, the CommManager provides 3 different sending operations, for different scenarios: publish, request and reply.
 * 
 * TODO:
 * 1. Add proper exceptions, and handle IO and reflection exceptions correctly.
 */
public class CommManager implements Runnable {
	// This is currently the only coupling between the protocol buffer definition and the messaging library
	// The values of this constant must identical to the name of the classname where all (protobuf) messages are defined
	private final static String MESSAGES_CLASSNAME = "Messages";
	
	// For each request, this is the amount of bytes which are used to identify it. This value will pass on the wire twice (request and reply), so
	// there is a trade-off between communication efficiency and the probability of collision for simulatenous requests.
	// 2 bytes seems like it would be enough (i.e., a single service isn't likely to have 10K simulatenous equivelent requests), but to make sure and for
	// better expansion potential I have set the value to 4. If we find out problems with out performance, we can check how tweaking this value effects it.
	private final static int REQUEST_ID_LENGTH = 4;
	private int next_request_id;
	private Object request_id_lock_object;
	
	private ZMQ.Context context;
	private ZMQ.Socket publisher;
	private ZMQ.Socket subscriber;
	private ZMQ.Socket router;
	private HashMap<String, ZMQ.Socket> dealers;
	private ZMQ.Poller polled_items;
	
	
	// Each message type the CommManager receives should have a MessageHandler set
	private HashMap<Descriptor, MessageHandler> subscriber_message_handlers;
	private HashMap<Descriptor, MessageHandler> router_message_handlers;
	
	// This hash is more complicated:
	// There can be multiple socket dealers
	// Each can have a multiple amount of active requests
	// Each has its own set of possible possible reply messages and their corresponding handler
	// In case a request has already been replied (important for handling timeouts), then dealers_message_handlers[socket][request_id] = null
	private HashMap<ZMQ.Socket, HashMap<Integer, HashMap<Descriptor, MessageHandler>>> dealers_message_handlers;
	
	private class RequestTimeoutMark implements Comparable<RequestTimeoutMark>
	{
		protected long timeout;
		protected int request_id;
		
		protected RequestTimeoutMark(long timeout, int request_id)
		{
			this.timeout = timeout;
			this.request_id = request_id;
		}
		
		@Override
		public int compareTo(RequestTimeoutMark o) {
			return Long.compare(timeout, o.timeout);
		}
	}
	private PriorityBlockingQueue<RequestTimeoutMark> request_timeouts;
	
	// Each type that is ever parsed is dynamically mapped to its appropriate parser method
	private HashMap<String, Method> type_parsers;

	
	/**
	 * Constructs a PubSubManager
	 */
	public CommManager()
	{
		context = ZMQ.context(1);
		subscriber_message_handlers = new HashMap<Descriptor, MessageHandler>();
		router_message_handlers = new HashMap<Descriptor, MessageHandler>();
		dealers_message_handlers = new HashMap<ZMQ.Socket, HashMap<Integer, HashMap<Descriptor, MessageHandler>>>();
		type_parsers = new HashMap<String, Method>();
		dealers = new HashMap<String, ZMQ.Socket>();
		polled_items = new ZMQ.Poller(2);
		next_request_id = 0;
		request_id_lock_object = new Object();
		request_timeouts = new PriorityBlockingQueue<RequestTimeoutMark>();
	}
	
	/**
	 * Closes socket, and ends context.
	 */
	public void close()
	{
		if (publisher != null)
		{
			publisher.close();
		}
		
		if (subscriber != null)
		{
			subscriber.close();
		}
		
		if (router != null)
		{
			router.close();
		}
		
		for (ZMQ.Socket dealer : dealers.values())
		{
			dealer.close();
		}
		
		context.term();
	}
	
	public void run()
	{
        //  Process messages from all sockets
        while (!Thread.currentThread ().isInterrupted ()) {
        	
        	// Poll for receiving a message
            polled_items.poll(200);
            
            int item_index;
            ZMQ.Socket socket;
            int size = polled_items.getSize();
            
            // Find out which socket receives the message
            for (item_index = 0; item_index < size; item_index++)
            {
            	if (polled_items.pollin(item_index))
            	{
            		byte[] peer_id = null;
            		HashMap<Descriptor, MessageHandler> message_handlers = null;
            		int request_id = -1;
            		
	            	socket = polled_items.getSocket(item_index);
	            	
	            	if (socket == router)
	            	{
	            		// Get the peer id, which is the first part of the message received by a router socket
	            		peer_id = router.recv();
	            		byte[] request_id_bytes = socket.recv();
	            		request_id = ByteBuffer.wrap(request_id_bytes).getInt();
	            		message_handlers = router_message_handlers;
	            	}
	            	else if (socket == subscriber)
	            	{
	            		message_handlers = subscriber_message_handlers;
	            	}
	            	else
	            	{
	            		// Socket is one of the dealers
	            		
	            		byte[] request_id_bytes = socket.recv();
	            		request_id = ByteBuffer.wrap(request_id_bytes).getInt();
	            		
	            		HashMap<Integer, HashMap<Descriptor, MessageHandler>> requests_message_handlers = dealers_message_handlers.get(socket);
	            		
	            		if (requests_message_handlers.containsKey(request_id))
	            		{
	            			message_handlers = requests_message_handlers.get(request_id);
	            			requests_message_handlers.put(request_id, null); // Null means that a reply was received
	            		}
	            		else
	            		{
	            			// This is a reply to an unexpected request (probably expired)
	            			System.out.println("No such request");
	            		}
	            	}
	            	  
	            	try
	            	{
	            		recvAndHandlePayload(socket, peer_id, request_id, message_handlers);
	            	}
	            	catch (MessageHandlerException ex)
	            	{
	            		System.out.println(ex.toString());
	            	}
            	}
            	
            	//checkRequestTimeouts();
            }
        }	
	}

	private void recvAndHandlePayload(ZMQ.Socket socket, byte[] peer_id, int request_id, HashMap<Descriptor, MessageHandler> message_handlers) throws MessageHandlerException {
		GeneratedMessage message;
                try {
                    message = recv(socket);
                } catch (MessageParsingException ex) {
                    throw new MessageHandlerException(ex.toString());
                }
		
		// Find the appropriatae MessageHandler for this type
		Descriptor message_type = message.getDescriptorForType();
		if (message_handlers.containsKey(message_type))
		{
			message_handlers.get(message_type).handleMessage(message, peer_id, request_id);
			
			/*
			 *  Possiblity: Make it so that the handler runs asynchronously on another thread
			 *  In the current state, incoming message handling is queued. Can pretty easily make it run on thread pool
			 *  It is a question of whether the communication layer should handle the asynchronous handling implicitly,
			 *  or the upper layers explicitly. Probably would like to make it configurable in the future.
			*/
		}
		else
		{
			throw new MessageHandlerException("No message handler defined for this message type");
		}
	}
	
	private void checkRequestTimeouts()
	{
		long current_timestamp = System.currentTimeMillis();
		RequestTimeoutMark next_timeout = request_timeouts.peek();
		while ((next_timeout != null) && (next_timeout.timeout < current_timestamp))
		{
			request_timeouts.poll();
			System.out.println();
		}
	}
	
	/**
	 * Binds the publishing to a specific address.
	 * @param addr	Address to bind for publishing.
	 */
	public void bindPublisher(String addr)
	{
		publisher = context.socket(ZMQ.PUB);
		publisher.bind(addr);
	}
	
	/**
	 * Binds the router to a specific address.
	 * @param addr - Address to bind
	 */
	public void listenOn(String addr)
	{
		if (router == null)
		{
			router = context.socket(ZMQ.ROUTER);
			polled_items.register(router, ZMQ.Poller.POLLIN);
		}
		
		router.bind(addr);
	}
	
	/**
	 * Binds the router to a specific address, and sets its list of handlers for different messages it may receive
	 * @param addr - Address to bind
	 * @param handlers - Map of request message types to their matching handlers
	 */
	public void listenOn(String addr, HashMap<Descriptor, MessageHandler> handlers)
	{
		listenOn(addr);
		setRequestHandler(handlers);
	}
	
	/**
	 * Set a MessageHandler for a specific type of request message. If a handler for this type already exists, this method will overwrite it
	 * @param message_descriptor - Request message type we want to set a handler for
	 * @param handler - The handler for this type
	 */
	public void setRequestHandler(Descriptor message_descriptor, MessageHandler handler)
	{
		router_message_handlers.put(message_descriptor, handler);
	}
	
	/**
	 * Set a full list of handlers for all request types. Will overwrite and previous handlers (or delete them if no new handler is set)
	 * @param handlers
	 */
	public void setRequestHandler(HashMap<Descriptor, MessageHandler> handlers)
	{
		router_message_handlers = handlers;
	}
	
	/**
	 * Connect the subscriber to a specific address.
	 * NOTE: This call alone dosen't actually subscribe to a message, so it is not enough to receive messages.
	 * @param addr	Address to connect to
	 */
	public void subscribeTo(String addr)
	{
		if (subscriber == null)
		{
			subscriber = context.socket(ZMQ.SUB);
			polled_items.register(subscriber, ZMQ.Poller.POLLIN);
		}
		
		subscriber.connect(addr);
	}
	
	/**
	 * Subscriber to receiving messages of a specific type (Descriptor). Also, explicitly pass a handler which will
	 * run on the messages of this type received.
	 * @param message_descriptor	Protobuf Descriptor of the message type to subscribe on.
	 * @param handler	An implementation of the MessageHandler, containing a method to handle the received messages
	 */
	public void subscribeTo(Descriptor message_descriptor, MessageHandler handler)
	{
		if (subscriber == null)
		{
			subscriber = context.socket(ZMQ.SUB);
			polled_items.register(subscriber, ZMQ.Poller.POLLIN);
		}
		
		// Set the handler for when receiving this message type. Override if handler already exists
		subscriber_message_handlers.put(message_descriptor, handler);
		
		String message_type = message_descriptor.getName();
		subscriber.subscribe(message_type.getBytes());
	}
	
	/**
	 * Connect the socket to an address, and subscribe to receiving messages of a specific type (Descriptor). 
	 * Also, explicitly pass a handler which will run on the messages of this type received.
	 * @param addr	Address to connect to
	 * @param message_descriptor	Protobuf Descriptor of the message type to subscribe on.
	 * @param handler	An implementation of the MessageHandler, containing a method to handle the received messages
	 */
	public void subscribeTo(String addr, Descriptor message_descriptor, MessageHandler handler)
	{
		subscribeTo(addr);
		subscribeTo(message_descriptor, handler);
	}
	
	/**
	 * Perform a request from another service. This assumes a reply will be returned, so pass a MessageHandler for the reply (callback pattern)
	 * TODO: Add support for timeout on the reply (perhaps an exception?)
	 * @param addr - The address to send the request to
	 * @param message - The request message
	 * @param reply_message_handlers - The list of handlers for different possible reply messages
	 * @param timeout - Timeout in milliseconds. Value of 0 means no timeout for this request
	 */
	public void request(String addr, GeneratedMessage message, HashMap<Descriptor, MessageHandler> reply_message_handlers, long timeout)
	{
		ZMQ.Socket dealer;
		
		int request_id;
		synchronized (request_id_lock_object) {
			// Must syncrhonize request id increment in case there are concurrent calls to the method
			request_id = ++next_request_id;
		}
		
		if (!dealers.containsKey(addr)) {
			dealer = context.socket(ZMQ.DEALER);
			dealers.put(addr, dealer);
			HashMap<Integer, HashMap<Descriptor, MessageHandler>> requests_to_handlers = new HashMap<Integer, HashMap<Descriptor, MessageHandler>>();
			requests_to_handlers.put(request_id, reply_message_handlers);
			dealers_message_handlers.put(dealer, requests_to_handlers);
			polled_items.register(dealer, ZMQ.Poller.POLLIN);
			dealer.connect(addr);
		}
		else
		{
			dealer = dealers.get(addr);
			
			// Check to see if the list of handlers is different from the one stored in memory. If so - update it
			// If the caller doesn't want to update the list of handlers, he can pass null as the value
			// TODO: Later, for the sake of performance, can separate the creation of dealer sockets for request-reply pattern from the sending itself
			if ((reply_message_handlers != null) && (!reply_message_handlers.equals(dealers_message_handlers.get(dealer))))
			{
				dealers_message_handlers.get(dealer).put(request_id, reply_message_handlers);
			}
		}
		
		// Get bytes of request id
		byte[] request_id_bytes = ByteBuffer.allocate(REQUEST_ID_LENGTH).putInt(next_request_id).array();
		
		// Send request id
		dealer.send(request_id_bytes, ZMQ.SNDMORE);
		// Send payload
		send(dealer, message);
		
		// Set timeout value after which the reply is invalid
		RequestTimeoutMark timeout_mark = new RequestTimeoutMark(System.currentTimeMillis() + timeout, request_id);
		request_timeouts.add(timeout_mark);
	}
	
	/**
	 * Syntactic sugar for request with no timeout set
	 * @param addr - The address to send the request to
	 * @param message - The request message
	 * @param reply_message_handlers - The list of handlers for different possible reply messages
	 */
	public void request(String addr, GeneratedMessage message, HashMap<Descriptor, MessageHandler> reply_message_handlers)
	{
		request(addr, message, reply_message_handlers, 0);
	}
	
	
	/**
	 * Syntactic sugar for the previous method. Most requests will have only one possible reply message type, so this method saves the caller
	 * the overhead of creating the hashtable himself.
	 * Also - if this is a request which has no reply, pass null at both last parameters
	 * @param addr - The address to send the request to
	 * @param message - The request message
	 * @param reply_descriptor - Type of the expected reply message
	 * @param reply_handler - Handler for the reply message
	 * @param timeout - Timeout in milliseconds. Value of 0 means no timeout for this request
	 */
	public void request(String addr, GeneratedMessage message, Descriptor reply_descriptor, MessageHandler reply_handler, int timeout)
	{
		HashMap<Descriptor, MessageHandler> handler_hashtable = new HashMap<Descriptor, MessageHandler>(1);
		if (reply_descriptor != null)
		{
			handler_hashtable.put(reply_descriptor, reply_handler);
		}
		request(addr, message, handler_hashtable, timeout);
	}
	
	/**
	 * Syntactic sugar for request with single handler and no timeout set
	 * @param addr - The address to send the request to
	 * @param message - The request message
	 * @param reply_descriptor - Type of the expected reply message
	 * @param reply_handler - Handler for the reply message
	 */
	public void request(String addr, GeneratedMessage message, Descriptor reply_descriptor, MessageHandler reply_handler)
	{
		request(addr, message, reply_descriptor, reply_handler, 0);
	}
	
	/**
	 * Sends a reply to answer another service's request
	 * NOTE: Should only be called from the MessageHandler of a request message - i.e., when another service performs a request on this CommManager
	 * @param message - Reply message
	 * @param peer_id - id of the peer to let the router socket know where to send the message
	 * @param request_id - id of the request we are replying to. Simply need to send it as well in order for the client to match the reply to request
         * @throws RouterException
	 */
	public void reply(GeneratedMessage message, byte[] peer_id, int request_id) throws RouterException
	{
		if (peer_id != null)
		{
			// First tell router which dealer to send the message
			router.send(peer_id, ZMQ.SNDMORE);
			byte[] request_id_bytes = ByteBuffer.allocate(REQUEST_ID_LENGTH).putInt(request_id).array();
			router.send(request_id_bytes, ZMQ.SNDMORE);
			send(router, message);
		}
		else
		{
			throw new RouterException("No peer id passed");
		}
	}
	
	/**
	 * Publish the message on the bus
	 * @param message
	 */
	public void publish(GeneratedMessage message)
	{
		send(subscriber, message);
	}
	
	/**
	 * Send (publish) a message of one of the defined types
	 * @param message	Message to serialze and send
	 */
	private void send(ZMQ.Socket socket, GeneratedMessage message)
	{
		// Sending is done as multi-part message
		
		// Send the message type in the first part. The subscription filter will only run on it
		String message_type = message.getDescriptorForType().getName();
		socket.send(message_type.getBytes(), ZMQ.SNDMORE);
		
		// Send the message bytes in teh second part
		byte[] message_bytes = message.toByteArray();
		socket.send(message_bytes);
	}
	
	
	/**
	 * Runs (in a blocking manner) until receiving a message of the type the manager has subscribed to.
	 * @return The message, whose runtime type is as it was sent on the other side
	 * @throws MessageParsingException
	 */
	private GeneratedMessage recv(ZMQ.Socket socket) throws MessageParsingException {
		// Get first part of message which contains message type
		byte[] message_type_bytes = socket.recv();
		String message_type = new String(message_type_bytes);
		
		// Get message bytes, and parse according to type
		byte[] message_bytes = socket.recv();
		GeneratedMessage message = parse(message_type, message_bytes);
	
		return message;
	}
	
	
	// Parses the serialized protobuf bytes according to the stated type
	private GeneratedMessage parse(String message_type, byte[] message_bytes) throws MessageParsingException {
                Method m = getParserMethod(message_type);
                try {
                    // Invoke the parser via reflection
                    return (GeneratedMessage) m.invoke(null, message_bytes);
                } catch (ReflectiveOperationException | IllegalArgumentException | 
                        NullPointerException | ExceptionInInitializerError ex) {
                    throw new MessageParsingException(ex.toString());
                }
		
	}
	
	// Gets the appropriate parser method for this message type
	// Note that the method is retrieved once via reflection, and then cached
	private Method getParserMethod(String message_type) throws MessageParsingException
	{
		if (type_parsers.containsKey(message_type))
		{
			return type_parsers.get(message_type);
		}
		else
		{
			Method parser_method;
			
			// We use the fact that the message type as passed in the protobuf is derived from the class name
			String full_classname = String.format("%s.%s$%s", this.getClass().getPackage().getName(), MESSAGES_CLASSNAME, message_type);
			
			try {
				Class<?> c = Class.forName(full_classname);		
				Class<?>[] parameter_types = {byte[].class};
				parser_method = c.getMethod("parseFrom", parameter_types);
			}
			catch(ClassNotFoundException | NoSuchMethodException | 
                                LinkageError | NullPointerException | SecurityException ex) {
				throw new MessageParsingException(ex.toString());
			}
			
			
			type_parsers.put(message_type, parser_method);
			return parser_method;
		}
	}
}
