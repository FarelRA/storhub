FROM golang:1.26.7-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .

ARG TARGETOS
ARG TARGETARCH
ARG TARGETVARIANT
ARG VERSION=dev
ENV CGO_ENABLED=0 \
    GOOS=$TARGETOS \
    GOARCH=$TARGETARCH
RUN case "$TARGETVARIANT" in "" | "v8") ;; \
      *) export GOARM="${TARGETVARIANT#v}" ;; \
    esac; \
    go build -trimpath \
      -ldflags "-s -w -X github.com/FarelRA/storhub/internal/cli.version=${VERSION}" \
      -o /out/storhub ./cmd/storhub

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/storhub /usr/bin/storhub
ENTRYPOINT ["/usr/bin/storhub"]
