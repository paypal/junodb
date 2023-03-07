#  
#  Copyright 2023 PayPal Inc.
#  
#  Licensed to the Apache Software Foundation (ASF) under one or more
#  contributor license agreements.  See the NOTICE file distributed with
#  this work for additional information regarding copyright ownership.
#  The ASF licenses this file to You under the Apache License, Version 2.0
#  (the "License"); you may not use this file except in compliance with
#  the License.  You may obtain a copy of the License at
#  
#     http://www.apache.org/licenses/LICENSE-2.0
#  
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#  
 
#!/bin/bash

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