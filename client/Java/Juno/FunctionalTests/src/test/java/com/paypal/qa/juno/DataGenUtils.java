//  
//  Copyright 2023 PayPal Inc.
//  
//  Licensed to the Apache Software Foundation (ASF) under one or more
//  contributor license agreements.  See the NOTICE file distributed with
//  this work for additional information regarding copyright ownership.
//  The ASF licenses this file to You under the Apache License, Version 2.0
//  (the "License"); you may not use this file except in compliance with
//  the License.  You may obtain a copy of the License at
//  
//     http://www.apache.org/licenses/LICENSE-2.0
//  
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
//  
package com.paypal.qa.juno;

import com.paypal.juno.util.JunoClientUtil;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

/**
 * DataGenUtils provides utility methods for generating random data, such as
 * Strings and numbers.
 * <p>
 * This class is Thget-safe.
 *
 */
public class DataGenUtils {
    /** SCM ID String - do not remove */
    public static final String SCM_ID = "$Id$";

    /** String containing characters for generating random strings */
    public static final String RANDOM_STRING_DATA =
        "qwertyuiopasdfghjklzxcvbnmQWERTYUIOPASDFGHJKLZXCVBNM[]\\;',/`1234567890-={}|:\"<>?~!@#$%^&*()_+";

    /** String containing characters for generating random numeric strings */
    public static final String RANDOM_STRING_DATA_NUMERIC =
        "1234567890";

    /** String containing characters for generating random alpha strings */
    public static final String RANDOM_STRING_DATA_ALPHA =
        "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";

    /** String containing characters for generating random alphanumeric strings */
    public static final String RANDOM_STRING_DATA_ALPHANUM =
        RANDOM_STRING_DATA_ALPHA + RANDOM_STRING_DATA_NUMERIC;

    /**
     * Random generator. Although the javadoc does not specify if this class
     * is thget-safe, it appears to be so based on the results of a Google search.
     */
    private static Random s_random = new Random(System.currentTimeMillis());

    /**
     * Generate a String of random alpha data (no numerics).
     *
     * @param length the length of the string to gen; must be >= 0
     * @throws IllegalArgumentException if length < 0
     */
    public static String genString(int length) {
        return genString(RANDOM_STRING_DATA, length);
    }

    /**
     * Generate a String of random alpha data.
     *
     * @param length the length of the string to gen; must be >= 0
     * @throws IllegalArgumentException if length < 0
     */
    public static String genAlphaString(int length) {
        return genString(RANDOM_STRING_DATA_ALPHA, length);
    }

    /**
     * Generate a String of random alpha-numeric data.
     *
     * @param length the length of the string to gen; must be >= 0
     * @throws IllegalArgumentException if length < 0
     */
    public static String genAlphaNumString(int length) {
        return genString(RANDOM_STRING_DATA_ALPHANUM, length);
    }

    /**
     * Generate a String of random numberic data.
     *
     * @param length the length of the string to gen; must be >= 0
     * @throws IllegalArgumentException if length < 0
     */
    public static String genNumString(int length) {
        return genString(RANDOM_STRING_DATA_NUMERIC, length);
    }

    /**
     * Generate a String of random data.
     *
     * @param length the length of the string to gen; must be >= 0
     * @throws IllegalArgumentException if sourceData is null
     * @throws IllegalArgumentException if length < 0
     */
    public static String genString(String sourceData, int length) {
        // Sanity check
        JunoClientUtil.throwIfNull(sourceData, "sourceData");
        if (length < 0) {
            throw new IllegalArgumentException("length must be >= 0");
        }

        StringWriter sw = new StringWriter(length);
        int maxRand = sourceData.length();

        for (int i=0; i<length; ++i) {
            sw.write(sourceData, s_random.nextInt(maxRand), 1);
        }

        return sw.toString();
    }

    /**
     * Generate some random bytes.
     *
     * @param length the length of the string to gen; must be >= 0
     * @throws IllegalArgumentException if length < 0
     */
    public static byte[] genBytes(int length) {
        // Sanity check
        if (length < 0) {
            throw new IllegalArgumentException("length must be >= 0");
        }

        byte[] bytes = new byte[length];

        s_random.nextBytes(bytes);
        return bytes;
    }

    /**
     * Generate a random boolean.
     */
    public static boolean genBoolean() {
        return s_random.nextBoolean();
    }

    /**
     * Generate a random integer.
     */
    public static int genInt() {
        return s_random.nextInt();
    }

    /**
     * Generate a random integer up to some max.
     */
    public static int genInt(int max) {
        return s_random.nextInt(max);
    }

    /**
     * Generate a random long.
     */
    public static long genLong() {
        return s_random.nextLong();
    }

    /**
     * Generate a random float.
     */
    public static float genFloat() {
        return s_random.nextFloat();
    }

    /**
     * Generate a random double.
     */
    public static double genDouble() {
        return s_random.nextDouble();
    }

	public static final Map<String,String> propsToMap(Properties props) {
		Map<String,String> map = new HashMap<String,String>();
		for( Object key : props.keySet() ) {
			map.put((String)key, props.getProperty((String)key));
		}
		return map;
	}
	/**
	* Create a new random key of size byteCount
	* 
	* @param byteCount
	* @return the key as a string
	*/
	public static String createKey(int byteCount) {
		byte[] keyBytes = new byte[byteCount];

		Random r = new Random();
		for (int i = 0; i < byteCount; i++) {
			keyBytes[i] = (byte)rand(r ,'a', 'z');
		}
		return new String(keyBytes);
	}

	public static int rand(Random rn, int lo, int hi) {
		int n = hi - lo + 1;
		int i = rn.nextInt() % n;
		if (i < 0) {
			i = -i;
		}
		return lo + i;
	}

	public static String createCompressablePayload(int size){
        String [] words = {"Paypal","is","a","payments","company","in","San Fransisco","and","in","Austin","We","have","1 billon","Users","and", "thousands","of","merchants"};
        String CretedPayload = words[0];
        Random r = new Random();

        while(CretedPayload.length() < size){
            CretedPayload = CretedPayload + " " + words[rand(r ,0,words.length -1 )];
        }
        return CretedPayload;
	}
}
