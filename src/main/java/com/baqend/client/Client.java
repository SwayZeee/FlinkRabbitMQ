package com.baqend.client;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

public interface Client {
    void doQuery(String query);

    void setup() throws Exception;

    void warmUp();

    void updateData(UUID uuid) throws IOException, TimeoutException;

    void deleteData();
}
