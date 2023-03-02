#!/bin/bash
cd "$(dirname "$0")"

# Generate TLS secrets
rm *.pem *.crt *.csr *.srl *.cfg

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

# echo "CA's self-signed certificate"
# openssl x509 -in ca.crt -noout -text

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

# Sign the certificate signing request
openssl x509 -req -in server.csr -days 3650 -CA ca.crt -CAkey ca.pem -CAcreateserial -out server.crt

# echo "Server's certificate"
# openssl x509 -in server.crt -noout -text

echo "Verify Server's Certificate with CA's certificate"
openssl verify -CAfile ca.crt server.crt


# Generate Encryption keys
cat << EOF > keystore.toml
# Sample Keystore
hexKeys = [
$(count=10;for i in $(seq $count); do echo \"`openssl rand -hex 32`\",;done)
]
EOF