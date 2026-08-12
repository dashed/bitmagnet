ARG WEBUI_REACT=false

# build app
FROM --platform=$BUILDPLATFORM golang:1.23.6-alpine3.20 AS app-builder-false
RUN apk add --no-cache git tzdata

ENV SERVICE=bitmagnet

WORKDIR /src

# Cache Go modules
COPY go.mod go.sum ./
RUN go mod download

COPY . ./

ARG VERSION=dev
ARG REVISION=dev
ARG BUILDTIME
ARG TARGETOS TARGETARCH TARGETVARIANT

RUN --network=none --mount=target=. \
export GOOS=$TARGETOS; \
export GOARCH=$TARGETARCH; \
[[ "$GOARCH" == "amd64" ]] && export GOAMD64=$TARGETVARIANT; \
[[ "$GOARCH" == "arm" ]] && [[ "$TARGETVARIANT" == "v6" ]] && export GOARM=6; \
[[ "$GOARCH" == "arm" ]] && [[ "$TARGETVARIANT" == "v7" ]] && export GOARM=7; \
echo $GOARCH $GOOS $GOARM$GOAMD64; \
go build -ldflags "-s -w -X github.com/bitmagnet-io/bitmagnet/internal/version.GitTag=${VERSION}" -o /build/bitmagnet main.go

# build React webui
FROM --platform=$BUILDPLATFORM node:22-alpine AS webui-react-builder

WORKDIR /src/webui-react

RUN corepack enable

COPY webui-react/package.json webui-react/pnpm-lock.yaml ./
RUN pnpm install --frozen-lockfile

COPY webui-react ./
RUN pnpm build

# build app with React webui
FROM --platform=$BUILDPLATFORM golang:1.23.6-alpine3.20 AS app-builder-true
RUN apk add --no-cache git tzdata

ENV SERVICE=bitmagnet

WORKDIR /src

# Cache Go modules
COPY go.mod go.sum ./
RUN go mod download

COPY . ./
COPY --from=webui-react-builder /src/webui-react/dist ./webui-react/dist

ARG VERSION=dev
ARG REVISION=dev
ARG BUILDTIME
ARG TARGETOS TARGETARCH TARGETVARIANT

# NO --mount=target=. here (unlike app-builder-false): the bind mount overlays
# the raw build context onto /src, SHADOWING the COPY'd webui-react/dist from
# the builder stage — go:embed then fails with 'no matching files found'.
RUN --network=none \
export GOOS=$TARGETOS; \
export GOARCH=$TARGETARCH; \
[[ "$GOARCH" == "amd64" ]] && export GOAMD64=$TARGETVARIANT; \
[[ "$GOARCH" == "arm" ]] && [[ "$TARGETVARIANT" == "v6" ]] && export GOARM=6; \
[[ "$GOARCH" == "arm" ]] && [[ "$TARGETVARIANT" == "v7" ]] && export GOARM=7; \
echo $GOARCH $GOOS $GOARM$GOAMD64; \
go build -tags webuireact -ldflags "-s -w -X github.com/bitmagnet-io/bitmagnet/internal/version.GitTag=${VERSION}" -o /build/bitmagnet main.go

FROM app-builder-${WEBUI_REACT} AS app-builder

# build runner
FROM alpine:latest AS runner

ARG REVISION=dev
ARG SOURCE_TREE=dev
ARG TAPE_ACQUISITION_PLAN_SHA256=sha256:c6febd6d4dbcc762050d5a4d38d401dc0d56f50f901b88fc252a382a83b455fe

LABEL org.opencontainers.image.source="https://github.com/bitmagnet-io/bitmagnet"
LABEL org.opencontainers.image.licenses="MIT"
LABEL org.opencontainers.image.base.name="alpine:latest"
LABEL org.opencontainers.image.revision="${REVISION}"
LABEL io.bitmagnet.source-tree="${SOURCE_TREE}"
LABEL io.bitmagnet.classifier-tape-contract="action-progress-processor-state-plan-v1"
LABEL io.bitmagnet.classifier-tape-acquisition-plan="${TAPE_ACQUISITION_PLAN_SHA256}"

RUN apk --no-cache add ca-certificates curl tzdata jq iproute2-ss

COPY --link --from=app-builder /build/bitmagnet* /usr/local/bin/
COPY --link testdata/parity/classifier-attach/t1/acquisition-plan.json /opt/bitmagnet/t1/acquisition-plan.json

RUN test "sha256:$(sha256sum /opt/bitmagnet/t1/acquisition-plan.json | cut -d ' ' -f 1)" = "${TAPE_ACQUISITION_PLAN_SHA256}"

ENTRYPOINT ["/usr/local/bin/bitmagnet"]
