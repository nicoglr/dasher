FROM golang:1.26-bookworm AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /dasher ./cmd/dasher

FROM gcr.io/distroless/static-debian12
COPY --from=build /dasher /dasher
ENTRYPOINT ["/dasher"]
