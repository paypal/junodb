[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)


- [TLS certificates/keys creation for Test](#tls-certificateskeys-creation)
  - [Quick Start](#quick-start)
  - [Make Certs](#make-certs)
  - [Encryption Keys](#encryption-keys)
- [Configure Sec in config.toml](#configure-secrets)


# TLS certificates/keys creation

> 

<!-- toc -->
## Quick Start

Run `gensecrets.sh` script. 

```bash
./gensecrets.sh
```

It generates following secrets
```
ca.crt # CA's certificate (valid for 3650 days)
ca.pem # CA's key
server.csr # Server's Cerficate Signing request
server.pem # Server's private key
server.crt # Server's certificate (self signed by CA generated in above steps - valid for 3650 days)

keystore.toml # Hex keys for encryption


```



## Make Certs

1. Generate CA's private key and self-signed certificate 
2. Generate web server's private key and certificate signing request (CSR)
3. Use CA's private key to sign web server's CSR and get the signed certificate that the server will use during TLS handshake

Generate certificates using openssl


- We create the CA's private key and self signed certificate. x509 option is used in openssl req command to generate the self signed certificate instead of CSR
  
```bash
cat << EOF > ca.cfg
[ req ]
prompt = no
distinguished_name = ca_dn

[ ca_dn ]
organizationName = "Juno Test"
commonName = "cert-authority"
countryName = "US"
stateOrProvinceName = "California"
EOF

# Generate the CA's self-signed certificate and private key
openssl req -x509 -newkey rsa:4096 -nodes -days 3650 -keyout ca.pem -out ca.crt -config ca.cfg
```

Use x509 command to display certificate information, convert certificates to various form, sign certificate requetss like a mini CA or edit certificate trust settings 
[Link](https://linux.die.net/man/1/x509)

```
openssl x509 -in ca-cert.pem -noout -text
```

- Next, generate the private key and certificate signing request for the server
```bash
cat << EOF > srv.cfg
[ req ]
prompt = no
distinguished_name = ca_dn

[ ca_dn ]
organizationName = "Juno Test"
commonName = "Juno-test-server"
countryName = "US"
stateOrProvinceName = "California"
EOF

# Generate server's CSR
openssl req -newkey rsa:4096 -nodes  -keyout server.pem -out server.csr -config srv.cfg
```

- Sign the certificate signing request
```bash
openssl x509 -req -in server.csr -days 60 -CA ca.crt -CAkey ca.pem -CAcreateserial -out server.crt
```

- Show "Server's certificate"
```bash
openssl x509 -in server.crt -noout -text
```

- "Verify Server's Certificate with CA's certificate"
```bash
openssl verify -CAfile ca.crt server.crt
```

## Encryption Keys

```bash

# Generate Encryption keys
cat << EOF > keystore.toml
# Sample Keystore
localAES256HexKeys = [
$(count=10;for i in $(seq $count); do echo \"`openssl rand -hex 32`\",;done)
]
EOF
```


## Configure Secrets

Juno proxy and storage both can use the use the secrets by cofiguring the Sec section in the config.toml 

Example
```toml
AppName = "junoserv"
IsNoKmsSecurity = true
KeyStoreFilePath = "./secrets/keystore.toml"
CertPemFilePath   = "./secrets/server.crt"
KeyPemFilePath    = "./secrets/server.pem"
```
