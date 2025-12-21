FROM alpine:latest

WORKDIR /app

COPY . .

CMD ["sh", "-c", "rm -rf ./processed_data/*"]