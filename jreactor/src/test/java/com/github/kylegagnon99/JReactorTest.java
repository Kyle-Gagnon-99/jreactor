package com.github.kylegagnon99;

class AppTest {

    public static void main(String[] args) {

        EventService evService = new EventService(true);
        evService.start();

        FullReactor fullReactor = new FullReactor(10);
        fullReactor.start();

        fullReactor.sendMessage(5, "Hello World!");

    }

}
