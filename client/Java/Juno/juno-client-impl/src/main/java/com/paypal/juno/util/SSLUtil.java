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
package com.paypal.juno.util;

import java.io.*;
import java.net.URL;
import java.security.*;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.security.interfaces.RSAPrivateKey;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import javax.net.ssl.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SSLUtil {
    private static Logger LOGGER = LoggerFactory.getLogger(SSLUtil.class);
    public static SSLContext getSSLContext() {
        SSLContext sslContext = null;
        InputStream crtInputStream = null;
        InputStream keyInputStream = null;
        URL crt = SSLUtil.class.getClassLoader().getResource("secrets/server.crt");
        URL key = SSLUtil.class.getClassLoader().getResource("secrets/server.pem");
        try {
            crtInputStream = crt.openStream();
            keyInputStream = key.openStream();
            if(crtInputStream != null && keyInputStream != null) LOGGER.info("Security Certificated Found! ");
            sslContext = createSSLFactory(keyInputStream, crtInputStream,"", "name");

        } catch (Exception e) {
            LOGGER.debug("Exception occured " + e.getMessage());
        }
        if(sslContext != null) LOGGER.info("SSLContext Instantiated! ");
        return sslContext;
    }

    public static SSLContext getSSLContext(String crtPath, String keyPath) {
        SSLContext sslContext = null;

        try {
            FileInputStream crtInputStream = new FileInputStream(crtPath);
            FileInputStream keyInputStream = new FileInputStream(keyPath);
            if(crtInputStream != null && keyInputStream != null) LOGGER.info("Security Certificated Found! ");
            sslContext = createSSLFactory(keyInputStream, crtInputStream,"", "name");
        } catch (Exception e) {
            LOGGER.debug("Exception occured " + e.getMessage());
        }
        if(sslContext != null) LOGGER.info("SSLContext Instantiated! ");

        return sslContext;
    }

    private static String readFileAsString(InputStream con) throws IOException {
        InputStreamReader st = new InputStreamReader(con, "utf-8");
        BufferedReader in = new BufferedReader(st);
        String content = "";
        do {
            String line = in.readLine();
            if (line == null) {
                break;
            }
            if (line.contains("BEGIN") && content.isEmpty()) {
                content += line + "\n";
            } else if (line.contains("END") && !content.isEmpty()) {
                content += "\n" + line;
            } else {
                content += line;
            }
        } while (true);
        return content;
    }

    private static SSLContext createSSLFactory(InputStream crtPath, InputStream keyPath, String password, String name) throws Exception {
        String privateKeyPem = readFileAsString(crtPath);
        String certificatePem = readFileAsString(keyPath);
        // Convert public certificates and private key into KeyStore
        final KeyStore keystore = createKeyStore(privateKeyPem, certificatePem, password, name);
        final KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        kmf.init(keystore, password.toCharArray());
        final KeyManager[] km = kmf.getKeyManagers();

        final SSLContext context = SSLContext.getInstance("TLS");
        X509TrustManager tm = new X509TrustManager() {
            public void checkClientTrusted(X509Certificate[] chain, String authType) {
                // Since we only use this in client side, this
                // method will not be used.
                LOGGER.info("No client cert verification");
            }
            public void checkServerTrusted(X509Certificate[] chain, String authType) {
                LOGGER.info("No server cert verification");
            }
            public X509Certificate[] getAcceptedIssuers() { return new X509Certificate[0]; }
        };
        TrustManager[] trustAllCerts = new TrustManager[] {tm};
        context.init(km, trustAllCerts, null);

        return context;
    }

    private static KeyStore createKeyStore(String privateKeyPem, String certificatePem, final String password, String name)
            throws Exception, KeyStoreException, IOException, NoSuchAlgorithmException, CertificateException {
        final X509Certificate[] cert = createCertificates(certificatePem);
        final KeyStore keystore = KeyStore.getInstance("JKS");
        keystore.load(null);
        // Import private key
        final PrivateKey key = createPrivateKey(privateKeyPem);
        keystore.setKeyEntry(name, key, password.toCharArray(), cert);
        return keystore;
    }


    private static PrivateKey createPrivateKey(String privateKeyPem) throws Exception {
        final Reader reader = new StringReader(privateKeyPem);
        BufferedReader r = new BufferedReader(reader);
        String s = r.readLine();
        if (s == null || !s.contains("BEGIN PRIVATE KEY")) {
            r.close();
            throw new IllegalArgumentException("No PRIVATE KEY found");
        }
        final StringBuilder b = new StringBuilder();
        s = "";
        while (s != null) {
            if (s.contains("END PRIVATE KEY")) {
                break;
            }
            b.append(s);
            s = r.readLine();
        }
        r.close();
        final String hexString = b.toString();
        final byte[] bytes = Base64.getDecoder().decode(hexString);
        return generatePrivateKeyFromDER(bytes);
    }

    private static X509Certificate[] createCertificates(String certificatePem) throws Exception {
        final List<X509Certificate> result = new ArrayList<X509Certificate>();
        final Reader reader = new StringReader(certificatePem);
        BufferedReader r = new BufferedReader(reader);
        String s = r.readLine();
        if (s == null || !s.contains("BEGIN CERTIFICATE")) {
            r.close();
            throw new IllegalArgumentException("No CERTIFICATE found");
        }
        StringBuilder b = new StringBuilder();
        while (s != null) {
            if (s.contains("END CERTIFICATE")) {
                String hexString = b.toString();
                final byte[] bytes = Base64.getDecoder().decode(hexString);
                X509Certificate cert = generateCertificateFromDER(bytes);
                result.add(cert);
                b = new StringBuilder();
            } else {
                if (!s.startsWith("----")) {
                    b.append(s);
                }
            }
            s = r.readLine();
        }
        r.close();

        return result.toArray(new X509Certificate[result.size()]);
    }

    private static RSAPrivateKey generatePrivateKeyFromDER(byte[] keyBytes) throws InvalidKeySpecException, NoSuchAlgorithmException {
        final PKCS8EncodedKeySpec spec = new PKCS8EncodedKeySpec(keyBytes);
        final KeyFactory factory = KeyFactory.getInstance("RSA");
        return (RSAPrivateKey) factory.generatePrivate(spec);
    }

    private static X509Certificate generateCertificateFromDER(byte[] certBytes) throws CertificateException {
        final CertificateFactory factory = CertificateFactory.getInstance("X.509");
        return (X509Certificate) factory.generateCertificate(new ByteArrayInputStream(certBytes));
    }


}
