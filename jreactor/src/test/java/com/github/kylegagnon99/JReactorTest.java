package com.github.kylegagnon99;

class AppTest {

    public static void main(String[] args) {

        EventService evService = new EventService(true);
        evService.start();

        FullReactor fullReactor = new FullReactor(5);
        fullReactor.start();

        String strToSend = "Hello World!";

        fullReactor.sendMessage(7, strToSend.getBytes());

    }

}
