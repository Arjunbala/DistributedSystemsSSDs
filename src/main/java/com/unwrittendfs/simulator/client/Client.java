package com.unwrittendfs.simulator.client;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class Client {
    private Integer clientLocation;
    private Integer clientId;

    public Integer getClientId() {
        return clientId;
    }

    public void setClientId(Integer clientId) {
        this.clientId = clientId;
    }

    public Integer getClientLocation() {
        return clientLocation;
    }

    public void setClientLocation(Integer clientLocation) {
        this.clientLocation = clientLocation;
    }
}
