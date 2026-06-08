FROM golang:1.23-alpine AS builder
WORKDIR /build
COPY go.mod ./
COPY main.go ./
RUN CGO_ENABLED=0 GOOS=linux go build -o /build/log-gen .

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=builder /build/log-gen /usr/local/bin/log-gen
ENTRYPOINT ["/usr/local/bin/log-gen"]
