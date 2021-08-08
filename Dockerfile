# Build the manager binary
FROM golang:1.16

ARG SYNC_INTERVAL
ARG TRANSPORT_TYPE
ARG DATABASE_URL

# kafka only needs these
ARG KAFKA_HOSTS
ARG KAFKA_ID

ENV GO111MODULE=on \
    GOOS=linux \
    GOARCH=amd64 \
    KAFKA_PRODUCER_HOSTS=$KAFKA_HOSTS \
    KAFKA_PRODUCER_ID=$KAFKA_ID \
    DATABASE_URL=$DATABASE_URL \
    HOH_TRANSPORT_SYNC_INTERVAL=$SYNC_INTERVAL \
    HOH_TRANSPORT_TYPE=$TRANSPORT_TYPE \
    POD_NAMESPACE="open-cluster-management"
 
WORKDIR /dist
COPY bin/hub-of-hubs-spec-transport-bridge hoh-spec-transport-bridge

CMD ["/dist/hoh-spec-transport-bridge"]
