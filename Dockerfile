FROM golang:1.26-alpine AS builder

WORKDIR /build

COPY go.mod go.sum ./
RUN go mod download

COPY . .

# Генерируем pb-файлы (если есть прото-зависимости)
# RUN go install google.golang.org/protobuf/cmd/protoc-gen-go@latest \
#  && go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest \
#  && go install github.com/grpc-ecosystem/grpc-gateway/v2/protoc-gen-grpc-gateway@latest \
#  && make proto-gen

RUN CGO_ENABLED=0 GOOS=linux go build \
    -ldflags="-w -s -extldflags '-static'" \
    -o /bin/searchsurge \
    ./cmd


FROM alpine:3.19 AS runtime

RUN apk --no-cache add ca-certificates bash curl

WORKDIR /app

COPY --from=builder /bin/searchsurge /app/searchsurge

RUN addgroup -g 10001 app && \
    adduser -u 10001 -G app -D -s /bin/false app && \
    chown -R app:app /app

USER app:app

EXPOSE 8080 9090 9091

HEALTHCHECK --interval=10s --timeout=3s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8080/health || exit 1

ENTRYPOINT ["/app/searchsurge"]