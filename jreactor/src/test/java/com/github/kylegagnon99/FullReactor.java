package com.github.kylegagnon99;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FullReactor extends Reactor {

    Logger logger = LoggerFactory.getLogger(this.getClass());

    public FullReactor(int rid) {
        super(rid);
    }

    public FullReactor(int rid, String socketAddress) {
        super(rid, socketAddress);
    }

    @Override
    protected void consumeMsg(byte[] message) {
        String messageString = new String(message);
        logger.info("Consume Message from Full Reactor {} with a message of {}", getRid(), messageString);

    }

    @Override
    protected void processFailMsg(String failMsgStr, long destRid, int numOfAttempts, byte[] originalMessage) {
        logger.info("Failed to deliver to {}", destRid);
        if(numOfAttempts <= 3) {
            resendMessage(destRid, numOfAttempts, originalMessage);
        }
    }

}
