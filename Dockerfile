FROM golang:1.27-alpine AS build
ARG GOPROXY=https://proxy.golang.org,direct
ENV GOPROXY=$GOPROXY
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /nexo ./cmd/nexo

FROM alpine:3.20
RUN apk add --no-cache ca-certificates tzdata && adduser -D -u 10001 nexo
COPY --from=build /nexo /usr/local/bin/nexo
USER nexo
EXPOSE 8080
ENTRYPOINT ["nexo"]
CMD ["serve", "-config", "/etc/nexo/config.yaml"]
