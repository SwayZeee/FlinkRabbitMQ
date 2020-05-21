package com.baqend.utils;

import org.asynchttpclient.AsyncHttpClient;

import static org.asynchttpclient.Dsl.*;

public class AHClient {

    private static AHClient singleton = null;
    private static AsyncHttpClient asyncHttpClient = asyncHttpClient();

    public static synchronized AHClient getInstance() {
        if (singleton == null) {
            singleton = new AHClient();
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
