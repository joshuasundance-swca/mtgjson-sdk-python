# syntax=docker/dockerfile:1.7

ARG PYTHON_VERSION=3.13

FROM python:${PYTHON_VERSION}-slim AS build

ENV PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /src

COPY pyproject.toml README.md LICENSE ./
COPY mtgjson_sdk ./mtgjson_sdk

RUN --mount=type=cache,target=/root/.cache/pip \
    python -m pip install --upgrade pip && \
    python -m pip install --prefix=/install ".[mcp]"


FROM python:${PYTHON_VERSION}-slim AS runtime

ARG UID=10001
ARG GID=10001
ARG VERSION=dev
ARG VCS_REF=unknown
ARG BUILD_DATE=unknown

LABEL org.opencontainers.image.title="mtgjson-sdk MCP server" \
      org.opencontainers.image.description="Docker-friendly MTGJSON FastMCP server with stdio-by-default and optional Streamable HTTP transport." \
      org.opencontainers.image.documentation="https://github.com/mtgjson/mtgjson-sdk-python#readme" \
      org.opencontainers.image.licenses="MIT" \
      org.opencontainers.image.created="${BUILD_DATE}" \
      org.opencontainers.image.source="https://github.com/mtgjson/mtgjson-sdk-python" \
      org.opencontainers.image.revision="${VCS_REF}" \
      org.opencontainers.image.version="${VERSION}" \
      io.modelcontextprotocol.server.name="io.github.mtgjson/mtgjson-sdk-python"

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    MTGJSON_MCP_CACHE_DIR=/data/mtgjson-cache \
    MTGJSON_MCP_HOST=0.0.0.0 \
    MTGJSON_MCP_LOG_LEVEL=WARNING

RUN groupadd --gid "${GID}" mtgjson && \
    useradd --uid "${UID}" --gid mtgjson --create-home --home-dir /home/mtgjson --shell /usr/sbin/nologin mtgjson && \
    mkdir -p /data/mtgjson-cache /workspace && \
    chown -R mtgjson:mtgjson /data /workspace /home/mtgjson

COPY --from=build /install /usr/local

WORKDIR /workspace
VOLUME ["/data"]
EXPOSE 8000

USER mtgjson

ENTRYPOINT ["mtgjson-mcp"]
CMD []
