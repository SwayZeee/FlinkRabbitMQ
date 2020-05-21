package com.baqend.utils;

import org.eclipse.jetty.client.util.StringContentProvider;
import org.eclipse.jetty.http.HttpMethod;

public class JettyHttpClient {

    private static JettyHttpClient singleton = null;
    private final static org.eclipse.jetty.client.HttpClient httpClient = new org.eclipse.jetty.client.HttpClient();

    public static synchronized JettyHttpClient getInstance() {
        if (singleton == null) {
            singleton = new JettyHttpClient();
            try {
                httpClient.start();
            } catch (Exception exception) {
                exception.printStackTrace();
            }
        }
        return singleton;
    }

    public void get(String url) {
        try {
            httpClient.newRequest(url)
                    .method(HttpMethod.GET)
                    .send();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void post(String url, String content) {
        try {
            httpClient.newRequest(url)
                    .method(HttpMethod.POST)
                    .header("Accept", "application/json")
                    .header("Content-type", "application/json")
                    .content(new StringContentProvider(content), "application/json")
                    .send();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void put(String url, String content) {
        try {
            httpClient.newRequest(url)
                    .method(HttpMethod.PUT)
                    .header("Accept", "application/json")
                    .header("Content-type", "application/json")
                    .content(new StringContentProvider(content), "application/json")
                    .send();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void delete(String url) {
        try {
            httpClient.newRequest(url)
                    .method(HttpMethod.DELETE)
                    .send();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void stop() {
        try {
            httpClient.stop();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
