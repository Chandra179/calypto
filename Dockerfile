FROM golang:latest AS builder

WORKDIR /app

# Download dependencies first (faster builds)
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY . .

# Build binary
RUN CGO_ENABLED=0 GOOS=linux go build -o main ./cmd/myapp

FROM alpine:latest

WORKDIR /app

# Copy binary from builder
COPY --from=builder /app/main .

# Expose app port (change if needed)
EXPOSE 8080

# Run the server
CMD ["./main"]
