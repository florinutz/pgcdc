FROM golang:1.25-alpine AS builder

ARG VERSION=dev

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -ldflags "-s -w -X github.com/florinutz/pgpipe/cmd.Version=${VERSION}" -o /pgpipe ./cmd/pgpipe

FROM alpine:3.21
RUN apk add --no-cache ca-certificates
COPY --from=builder /pgpipe /usr/local/bin/pgpipe
ENTRYPOINT ["pgpipe"]
