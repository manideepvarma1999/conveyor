# Static binary on a distroless base: no shell, no package manager, non-root user.
FROM golang:1.25 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/conveyor ./cmd/conveyor

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/conveyor /conveyor
USER nonroot:nonroot
EXPOSE 8080
ENV ADDR=0.0.0.0:8080
ENTRYPOINT ["/conveyor"]
CMD ["serve"]
