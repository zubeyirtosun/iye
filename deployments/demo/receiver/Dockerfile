FROM golang:1.23-alpine AS builder
WORKDIR /build
COPY go.mod ./
COPY main.go ./
RUN CGO_ENABLED=0 GOOS=linux go build -o /build/receiver .

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=builder /build/receiver /usr/local/bin/receiver
ENTRYPOINT ["/usr/local/bin/receiver"]
