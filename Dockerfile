# Build stage
FROM golang:1.21-alpine AS builder

WORKDIR /app

# Copy go.mod and go.sum
COPY go.mod go.sum ./

# Download dependencies
RUN go mod download

# Copy source code
COPY . .

# Build binary (build whole app package, not just main.go)
RUN CGO_ENABLED=0 GOOS=linux go build -o redis-server ./app

# Runtime stage
FROM alpine:latest

WORKDIR /app

# Copy binary from builder
COPY --from=builder /app/redis-server .

# Expose Redis port
EXPOSE 6379

# Run server
CMD ["./redis-server"]
