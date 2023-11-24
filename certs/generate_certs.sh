openssl genrsa -out server.key 4096
openssl req -new -key server.key -out server.csr 
openssl x509 -req -days 365 -req -in server.csr -signkey server.key -set_serial 01 -out server.crt 
openssl rsa -in server.key -out server.key
