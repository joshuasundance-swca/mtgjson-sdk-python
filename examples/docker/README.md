# Docker MCP Examples

This directory collects copy-pasteable Docker examples for the MTGJSON MCP
server.

The examples assume the published image is:

```bash
IMAGE=ghcr.io/mtgjson/mtgjson-sdk-python:latest
```

If you publish the image under a different tag or registry, replace `IMAGE`
accordingly.

## Quick starts

### Default `stdio` server

```bash
docker run --rm -i $IMAGE
```

### `stdio` with a persistent cache volume

```bash
docker run --rm -i \
  -v mtgjson-cache:/data \
  -e MTGJSON_MCP_CACHE_DIR=/data/mtgjson-cache \
  $IMAGE
```

### HTTP mode

```bash
docker run --rm \
  -p 8000:8000 \
  -v mtgjson-cache:/data \
  -e MTGJSON_MCP_TRANSPORT=http \
  -e MTGJSON_MCP_HOST=0.0.0.0 \
  -e MTGJSON_MCP_PORT=8000 \
  -e MTGJSON_MCP_PATH=/mcp \
  -e MTGJSON_MCP_CACHE_DIR=/data/mtgjson-cache \
  $IMAGE
```

Then connect your MCP client to `http://127.0.0.1:8000/mcp`.

## Files in this folder

* `compose.http.yaml` runs the MCP server as a long-lived HTTP container with a
  persistent Docker volume.
* `mcp-config.generic.json` shows a generic MCP client config that shells out to
  `docker run` for the default `stdio` transport.

## Notes

* Use `-i` for `stdio` transport so Docker keeps `STDIN` open.
* Skip `-t` for machine-to-machine MCP traffic unless your specific client
  explicitly needs a TTY.
* Warm profiles are best reserved for long-lived HTTP containers or repeated
  launches backed by the same cache volume.
