FROM alpine:latest

WORKDIR /app

COPY . .

CMD ["sh", "-c", "rm -rf ./raw_data/*"]