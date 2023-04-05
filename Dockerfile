FROM docker.stackable.tech/soenkeliebau/ycsb:latest
RUN microdnf install krb5-workstation openssl
