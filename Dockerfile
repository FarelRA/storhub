# Standard source build - works anywhere: docker build .
# Cross-compiles inside BuildKit via the standard TARGETOS/TARGETARCH
# arguments, so it also produces correct images on foreign bases.
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
# "v8" is a no-op arm on purpose: GOARM applies to 32-bit ARM only, but
# the base image reports variant v8 on arm64, so match it explicitly
# instead of exporting a bogus GOARM.
RUN case "$TARGETVARIANT" in "" | "v8") ;; \
      *) export GOARM="${TARGETVARIANT#v}" ;; \
    esac; \
    go build -trimpath \
      -ldflags "-s -w -X github.com/FarelRA/storhub/internal/cli.version=${VERSION}" \
      -o /out/storhub ./cmd/storhub

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/storhub /usr/bin/storhub
ENTRYPOINT ["/usr/bin/storhub"]
