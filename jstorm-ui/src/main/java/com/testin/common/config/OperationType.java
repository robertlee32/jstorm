package com.testin.common.config;

/**
 * @author rollinkin
 * @date 2015-03-09
 */
public enum OperationType {
    WRITE(true),

    READ(false);

    private boolean type;

    OperationType(boolean type) {
        this.type = type;
    }

    public boolean isType() {
        return type;
    }
}
