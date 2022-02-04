package com.github.kylegagnon99;

import com.github.kylegagnon99.MsgAttemptsOuterClass.MsgAttempts;
import com.github.kylegagnon99.ReactorIdOuterClass.ReactorId;
import com.google.protobuf.InvalidProtocolBufferException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMQException;
import org.zeromq.ZMQ.Socket;

/**
 * A class that can send messages and react to incoming ones. The logic on what to do with a message
 * must be implemented along with implementing what happens on a failed to deliver message.
 */
public abstract class Reactor extends Thread {

    Logger logger = LoggerFactory.getLogger(this.getClass());

    private SocketType socketType = SocketType.DEALER;
    private String socketAddress = "tcp://127.0.0.1:5555";
    private ZContext context = new ZContext();
    private Socket dealerSocket;

    private int reactorId;
    private boolean isRunning = true;

    /**
     * Sets up the reactor class by setting the socket's id, type, and address.
     * After setting up the socket it then connects it to the address.Uses the default
     * address of tcp://127.0.0.0.1:5555.
     *
     * @param reactorId The reactor id
     */
    public Reactor(int reactorId) {

        this.reactorId = reactorId;

        ReactorIdOuterClass.ReactorId reactorIdObj = ReactorIdOuterClass.ReactorId.newBuilder().setReactorId(reactorId).build();
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
     * @param reactorId The reactor id
     * @param socketAddress The address to connect to
     */
    public Reactor(int reactorId, String socketAddress) {

        this.reactorId = reactorId;

        ReactorId reactorIdObj = ReactorId.newBuilder().setReactorId(reactorId).build();
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
     * Starts the Reactor class
     */
    @Override
    public void run() {

        byte[] message;
        byte[] destMsg;
        byte[] numberOfAttemptsMsg;
        byte[] originalMsg;

        while(isRunning) {

            message = dealerSocket.recv(ZMQ.DONTWAIT);
            if(message == null) {
                continue;
            }

            String messageString = new String(message, ZMQ.CHARSET);

            if(messageString.equals(MessageTypes.FAIL_TO_DELIVER.value)) {
                destMsg = dealerSocket.recv();
                numberOfAttemptsMsg = dealerSocket.recv();
                originalMsg = dealerSocket.recv();

                ReactorId destReactorIdObj = null;
                try {
                    destReactorIdObj = ReactorId.parseFrom(destMsg);
                } catch (InvalidProtocolBufferException e) {
                    e.printStackTrace();
                }

                MsgAttempts msgAttemptsObj = null;
                try {
                    msgAttemptsObj = MsgAttempts.parseFrom(numberOfAttemptsMsg);
                } catch (InvalidProtocolBufferException e) {
                    e.printStackTrace();
                }

                processFailMsg(messageString, destReactorIdObj.getReactorId(), msgAttemptsObj.getNumOfAttempts(), originalMsg);
            } else {
                consumeMsg(message);
            }

        }

    }

    /**
     * Sends a message to the specified reactor.
     * @param destRid The reactor to send to
     * @param message The message to send
     */
    public void sendMessage(long destRid, byte[] message) {

        ReactorId reactorIdObj = ReactorId.newBuilder().setReactorId(destRid).build();
        byte[] destRidBytes = reactorIdObj.toByteArray();

        MsgAttempts numOfAttempts = MsgAttempts.newBuilder().setNumOfAttempts(0).build();
        byte[] numOfAttemptsBytes = numOfAttempts.toByteArray();

        dealerSocket.send(destRidBytes, ZMQ.SNDMORE);
        dealerSocket.send(numOfAttemptsBytes, ZMQ.SNDMORE);
        dealerSocket.send(message);
    }

    /**
     * Resends a message with the current amount of attempts
     * @param destRid - The destination of the message
     * @param numOfAttempts - The number of attempts the message has tried to send but failed
     * @param message - The original message
     */
    protected void resendMessage(long destRid, int numOfAttempts, byte[] message) {
        ReactorId destIdObj = ReactorId.newBuilder().setReactorId(destRid).build();
        byte[] destRidBytes = destIdObj.toByteArray();

        MsgAttempts numOfAttemptsObj = MsgAttempts.newBuilder().setNumOfAttempts(numOfAttempts).build();
        byte[] numOfAttemptsBytes = numOfAttemptsObj.toByteArray();

        dealerSocket.send(destRidBytes, ZMQ.SNDMORE);
        dealerSocket.send(numOfAttemptsBytes, ZMQ.SNDMORE);
        dealerSocket.send(message);
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
        return this.reactorId;
    }

    /**
     * A method to consume the incoming message. This is where the real power comes from.
     * @param message
     */
    protected abstract void consumeMsg(byte[] message);

    /**
     * How to process a fail delivery (i.e the receiving client is not up or the router could
     * not find a path to deliver to)
     * @param failMsgStr The failure message
     * @param destId Where it was supposed to go
     */
    protected abstract void processFailMsg(String failMsgStr, long destId, int numberOfAttempts, byte[] originalMsg);


}
