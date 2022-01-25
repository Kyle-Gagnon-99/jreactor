package com.github.kylegagnon99;

import java.nio.ByteBuffer;

import com.github.kylegagnon99.ReactorIdOuterClass.ReactorId;
import com.google.protobuf.InvalidProtocolBufferException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMQException;
import org.zeromq.ZMQ.Socket;

public abstract class Reactor extends Thread {

    Logger logger = LoggerFactory.getLogger(this.getClass());

    private SocketType socketType = SocketType.DEALER;
    private String socketAddress = "tcp://127.0.0.1:5555";
    private ZContext context = new ZContext();
    private Socket dealerSocket;

    private int rid;
    private boolean isRunning = true;

    /**
     * Sets up the reactor class by setting the socket's id, type, and address.
     * After setting up the socket it then connects it to the address.
     *
     * Uses the default address of tcp://127.0.0.0.1:5555
     *
     * @param rid The reactor id
     */
    public Reactor(int rid) {

        this.rid = rid;

        ReactorIdOuterClass.ReactorId reactorIdObj = ReactorIdOuterClass.ReactorId.newBuilder().setRid(rid).build();
        byte[] reactorIdBytes = reactorIdObj.toByteArray();

        dealerSocket = context.createSocket(socketType);
        dealerSocket.setIdentity(reactorIdBytes);

        try {
            dealerSocket.connect(socketAddress);
        } catch(ZMQException exception) {
            logger.error(exception.toString());
        }

    }

    /**
     * Sets up the reactor class by setting the socket's id, type, and address.
     * After setting up the socket it then connects it to the address.
     *
     * @param rid The reactor id
     * @param socketAddress The address to connect to
     */
    public Reactor(int rid, String socketAddress) {

        this.rid = rid;

        ReactorId reactorIdObj = ReactorId.newBuilder().setRid(rid).build();
        byte[] reactorIdBytes = reactorIdObj.toByteArray();

        dealerSocket = context.createSocket(socketType);
        dealerSocket.setIdentity(reactorIdBytes);

        try {
            dealerSocket.connect(socketAddress);
        } catch(ZMQException exception) {
            logger.error(exception.toString());
        }

    }

    /**
     * Sends a message to the specified reactor.
     * @param destRid The reactor to send to
     * @param message The message to send
     */
    public void sendMessage(int destRid, String message) {

        ReactorId reactorIdObj = ReactorId.newBuilder().setRid(destRid).build();
        byte[] destRidBytes = reactorIdObj.toByteArray();

        dealerSocket.send(destRidBytes, ZMQ.SNDMORE);
        dealerSocket.send(message.getBytes(ZMQ.CHARSET));
    }

    /**
     * Starts the Reactor class
     */
    @Override
    public void run() {

        byte[] message;
        byte[] destMsg;

        while(isRunning) {

            message = dealerSocket.recv(ZMQ.DONTWAIT);
            if(message == null) {
                continue;
            }

            String messageString = new String(message, ZMQ.CHARSET);

            if(messageString.equals(MessageTypes.FAIL_TO_DELIVER.value)) {
                destMsg = dealerSocket.recv();
                ReactorId destReactorIdObj = null;
                try {
                    destReactorIdObj = ReactorId.parseFrom(destMsg);
                } catch (InvalidProtocolBufferException e) {
                    e.printStackTrace();
                }

                processFailMsg(messageString, destReactorIdObj.getRid());
            } else {
                consumeMsg(messageString);
            }

        }

    }

    /**
     * Closes the socket, does any resource clean up, and stops the thread.
     */
    public void stopThread() {
        isRunning = false;
        dealerSocket.close();

        try {
            this.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * Returns the reactor id of this reactor. Can't set because it can't change after object
     * construction.
     * @return The reactor id
     */
    public int getRid() {
        return this.rid;
    }

    /**
     * A method to consume the incoming message. This is where the real power comes from.
     * @param message
     */
    protected abstract void consumeMsg(String message);

    /**
     * How to process a fail delivery (i.e the receiving client is not up or the router could
     * not find a path to deliver to)
     * @param failMsgStr The failure message
     * @param destId Where it was supposed to go
     */
    protected abstract void processFailMsg(String failMsgStr, long destId);


}
