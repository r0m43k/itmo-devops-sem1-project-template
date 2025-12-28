FROM golang:1.23 AS build
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN go build -o project_sem .

FROM debian:bookworm-slim
WORKDIR /app
COPY --from=build /app/project_sem /app/project_sem
EXPOSE 8080
CMD ["/app/project_sem"]
