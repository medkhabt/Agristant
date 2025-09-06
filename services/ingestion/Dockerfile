FROM golang:1.24 AS build 
WORKDIR /src 
COPY . . 
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -ldflags="-s -w" -o api

FROM gcr.io/distroless/static:nonroot
WORKDIR /data
WORKDIR /app
COPY --from=build /src/api /app/api
USER nonroot
EXPOSE 8080
ENTRYPOINT ["/app/api"]
