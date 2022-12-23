FROM golang:1.18-alpine

# Layer 1: baseline dependencies and utilities
RUN apk add docker-cli bash postgresql-client build-base git

# Layer 2: build dependencies
RUN go install github.com/golang/mock/mockgen@v1.6.0