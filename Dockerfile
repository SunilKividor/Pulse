FROM golang:alpine3.21 AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY *.go ./
COPY .env ./

RUN CGO_ENABLED=0 GOOS=linux go build -o /pulse-docker

FROM alpine:3.21

COPY --from=builder /pulse-docker /pulse-docker
COPY --from=builder /app/.env .

CMD ["/pulse-docker"]