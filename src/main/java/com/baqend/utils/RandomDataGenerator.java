package com.baqend.utils;

import org.apache.commons.lang3.RandomStringUtils;

import java.util.HashMap;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

public class RandomDataGenerator {

    public HashMap<String, String> generateRandomDataset() {
        HashMap<String, String> data = new HashMap<String, String>();
        data.put("fieldOne", Integer.toString(generateRandomInteger(1, 1000)));
        data.put("fieldTwo", Double.toString(generateRandomDouble(1, 1000)));
        data.put("fieldThree", generateRandomString(6, true, false));
        data.put("fieldFour", Integer.toString(generateRandomInteger(1001, 10000)));
        data.put("fieldFive", Double.toString(generateRandomDouble(1001, 10000)));
        data.put("fieldSix", generateRandomString(12, true, false));
        data.put("fieldSeven", Integer.toString(generateRandomInteger(10001, 100000)));
        data.put("fieldEight", Double.toString(generateRandomDouble(10001, 100000)));
        data.put("fieldNine", generateRandomString(18, true, false));
        return data;
    }

    public HashMap<String, String> generateRandomDataset(int i) {
        HashMap<String, String> data = new HashMap<String, String>();
        data.put("fieldOne", Integer.toString(generateRandomInteger(1, 1000)));
        data.put("fieldTwo", Double.toString(generateRandomDouble(1, 1000)));
        data.put("fieldThree", generateRandomString(6, true, false));
        data.put("fieldFour", Integer.toString(generateRandomInteger(1001, 10000)));
        data.put("fieldFive", Double.toString(generateRandomDouble(1001, 10000)));
        data.put("fieldSix", generateRandomString(12, true, false));
        data.put("fieldSeven", Integer.toString(generateRandomInteger(10001, 100000)));
        data.put("fieldEight", Double.toString(generateRandomDouble(10001, 100000)));
        data.put("fieldNine", generateRandomString(18, true, false));
        data.put("number", Integer.toString(i));
        return data;
    }

    public int generateRandomInteger(int min, int max) {
        return ThreadLocalRandom.current().nextInt(min, max + 1);
    }

    public double generateRandomDouble(int min, int max) {
        return ThreadLocalRandom.current().nextDouble(min, max + 1);
    }

    public String generateRandomString(int length, boolean letters, boolean numbers) {
        return RandomStringUtils.random(length, letters, numbers);
    }
}
