package com.github.kylegagnon99;

public enum MessageTypes {

    FAIL_TO_DELIVER("FAIL_TO_DELIVER");

    public final String value;

    private MessageTypes(String p_value) {
        this.value = p_value;
    }

}
