package com.github.kylegagnon99;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMQException;
import org.zeromq.ZMQ.Socket;

/**
 * Starts the service that will act as the router for all messages connected to this
 * binded endpoint.
 */
public class EventService extends Thread {

    Logger logger = LoggerFactory.getLogger(this.getClass());

    /**
     * Private Variables
     */
    private SocketType socketType = SocketType.ROUTER;
    private String socketAddress = "tcp://127.0.0.1:5555";
    ZContext context = new ZContext();

    private Socket routerSocket;
    private boolean bindingFailed = false;

    /**
     * Constructs the EventService object.
     *
     * Upon construction of the object, the socket will be created
     * with the type and it's address and binds the address. Does not
     * start the thread.
     *
     * Uses the default address of tcp://127.0.0.1:5555
     *
     * @param showBindingFailedMsg Whether or not to show a debug message that the binding of the port failed
     */
    public EventService(boolean showBindingFailedMsg) {

        routerSocket = context.createSocket(socketType);
        routerSocket.setRouterMandatory(true);
        try {
            routerSocket.bind(socketAddress);
        } catch(Exception e) {
            bindingFailed = true;
            if (showBindingFailedMsg) {
                logger.warn(e.toString());
            }
        }
    }

    /**
     * Constructs the EventService object.
     *
     * Upon construction of the object, the socket will be created
     * with the type and it's address and binds the address. Does not
     * start the thread.
     *
     * @param showBindingFailedMsg Whether or not to show a debug message that the binding of the port failed
     * @param p_socketAddr The socket address to bind to
     */
    public EventService(boolean showBindingFailedMsg, String socketAddress) {

        routerSocket = context.createSocket(socketType);
        routerSocket.setRouterMandatory(true);

        try {
            routerSocket.bind(socketAddress);
        } catch(Exception e) {
            bindingFailed = true;
            if (showBindingFailedMsg) {
                logger.warn(e.toString());
            }
        }
    }

    /**
     * Starts the event service by listening at the specified address
     */
    @Override
    public void run() {

        while(true) {

            byte[] sourceMsg = routerSocket.recv(ZMQ.DONTWAIT);

            if(sourceMsg == null) {
                continue;
            }

            byte[] destMsg = routerSocket.recv();
            byte[] message = routerSocket.recv();

            try {
                passMessage(destMsg, message);
            } catch(ZMQException exception) {
                logger.warn(exception.toString());
                sendFailMsg(sourceMsg, destMsg);
            }

        }

    }

    /**
     * Takes the incoming message and passes along the message to where it is supposed to go
     * @param destMsg The destination of the message
     * @param message The actual message
     */
    private void passMessage(byte[] destMsg, byte[] message) {

        routerSocket.send(destMsg, ZMQ.SNDMORE);
        routerSocket.send(message);

    }

    /**
     * Sends a failed message back to where it came from if the router can't find the client
     * @param sourceMsg Where the message came from
     * @param destMsg Where it was supposed to go
     */
    private void sendFailMsg(byte[] sourceMsg, byte[] destMsg) {

        String failMsgString = MessageTypes.FAIL_TO_DELIVER.value;

        routerSocket.send(sourceMsg, ZMQ.SNDMORE);
        routerSocket.send(failMsgString.getBytes(ZMQ.CHARSET), ZMQ.SNDMORE);
        routerSocket.send(destMsg);

    }


    /**
     * Returns if the binding of socket failed or not
     * @return Whether or not binding of the socket failed
     */
    public boolean bindingFailed() {
        return bindingFailed;
    }

    /**
     * Gets the socket type of the current object's socket
     * @return The socket type
     */
    public SocketType getSocketType() {
        return this.socketType;
    }

    /**
     * Gets the current socket's address.
     * @return The socket's address
     */
    public String getSocketAddress() {
        return this.socketAddress;
    }

}
