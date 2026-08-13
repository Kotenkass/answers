FROM golang:1.25-alpine AS build

WORKDIR /src

RUN apk add --no-cache git ca-certificates

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /out/answers .

FROM alpine:3.20

RUN apk add --no-cache ca-certificates tzdata

WORKDIR /app
COPY --from=build /out/answers /app/answers

USER 65532:65532
EXPOSE 8080
ENTRYPOINT ["/app/answers"]
