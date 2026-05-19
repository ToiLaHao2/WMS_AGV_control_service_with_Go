# ============================================================
# WMSS AGV Control Service (Go) — Multi-stage Build
# Stage 1: Build binary
# Stage 2: Run (scratch — siêu nhẹ ~10MB)
# ============================================================

# --- Stage 1: Build ---
FROM golang:1.25-alpine AS builder

WORKDIR /app

# Copy go.mod và go.sum trước → cache dependencies
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY . .

# Tự động cập nhật các thư viện còn thiếu (như kafka-go)
RUN go mod tidy

# Build binary tĩnh (static binary, không cần libc)
RUN CGO_ENABLED=0 GOOS=linux go build -o /agv-service ./cmd/main.go

# --- Stage 2: Run ---
FROM alpine:latest

# Cài ca-certificates cho HTTPS calls (nếu cần)
RUN apk --no-cache add ca-certificates

WORKDIR /app

# Copy binary từ builder stage
COPY --from=builder /agv-service .

EXPOSE 8081

CMD ["./agv-service"]
