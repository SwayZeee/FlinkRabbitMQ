package com.baqend.utils;

import org.asynchttpclient.AsyncHttpClient;

import static org.asynchttpclient.Dsl.*;

public class AHCAsyncHttpClient {

    private static AHCAsyncHttpClient singleton = null;
    private static final AsyncHttpClient asyncHttpClient = asyncHttpClient();

    public static synchronized AHCAsyncHttpClient getInstance() {
        if (singleton == null) {
            singleton = new AHCAsyncHttpClient();
        }
        return singleton;
    }

    public void get(String url) {
        try {
            asyncHttpClient.prepareGet(url)
                    .execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void post(String url, String content) {
        try {
            asyncHttpClient.preparePost(url)
                    .setHeader("Accept", "application/json")
                    .setHeader("Content-type", "application/json")
                    .setBody(content)
                    .execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void put(String url, String content) {
        try {
            asyncHttpClient.preparePut(url)
                    .setHeader("Accept", "application/json")
                    .setHeader("Content-type", "application/json")
                    .setBody(content)
                    .execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void delete(String url) {
        try {
            asyncHttpClient.prepareDelete(url)
                    .execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void stop() {
        try {
            asyncHttpClient.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
