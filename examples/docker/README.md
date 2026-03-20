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

### Docker Compose with MCP Inspector

```bash
docker compose -f examples/docker/compose.inspector.yaml up
```

This starts:

* `mtgjson-mcp` as a private HTTP server on the Compose network
* `mcp-inspector` as a localhost-bound browser UI and proxy on ports `6274`
  and `6277`

Open `http://127.0.0.1:6274/` and connect with:

* Transport: `Streamable HTTP`
* Server URL: `http://mtgjson-mcp:8000/mcp`
* Proxy auth token: the value of `MCP_INSPECTOR_TOKEN`

If you do not set `MCP_INSPECTOR_TOKEN`, this example uses the local-only
development default `mtgjson-local-dev-token`.

To test the image built from your current checkout instead of the published
image:

```bash
docker build -t mtgjson-sdk:local .
IMAGE=mtgjson-sdk:local docker compose -f examples/docker/compose.inspector.yaml up
```

For a pre-filled local browser URL, use:

```text
http://127.0.0.1:6274/?MCP_PROXY_AUTH_TOKEN=mtgjson-local-dev-token&transport=streamable-http&serverUrl=http%3A%2F%2Fmtgjson-mcp%3A8000%2Fmcp
```

Stop the sidecar stack with:

```bash
docker compose -f examples/docker/compose.inspector.yaml down
```

## Files in this folder

* `compose.http.yaml` runs the MCP server as a long-lived HTTP container with a
  persistent Docker volume. Set `IMAGE` first if you want to reuse a local tag
  instead of the published image.
* `compose.inspector.yaml` runs the MTGJSON HTTP server with MCP Inspector as a
  sidecar. The MCP server stays private to the Compose network while the
  Inspector UI and proxy bind only to `127.0.0.1`.
* `mcp-config.generic.json` shows a generic MCP client config that shells out to
  `docker run` for the default `stdio` transport.

## Notes

* Use `-i` for `stdio` transport so Docker keeps `STDIN` open.
* Skip `-t` for machine-to-machine MCP traffic unless your specific client
  explicitly needs a TTY.
* Warm profiles are best reserved for long-lived HTTP containers or repeated
  launches backed by the same cache volume.
* Both Compose files accept `IMAGE` as an override. For example, use
  `IMAGE=mtgjson-sdk:local` after building a local test tag.
* Both Compose files reuse the named Docker volume `mtgjson-mcp-cache`, so
  switching between the plain HTTP stack and the Inspector sidecar does not
  force a full cache re-download.
* The Inspector sidecar keeps proxy authentication enabled. If you want a custom
  token, set `MCP_INSPECTOR_TOKEN` before `docker compose up`.
* The Inspector proxy can launch local MCP commands and connect to arbitrary MCP
  servers, so the sidecar example keeps it bound to `127.0.0.1` for local
  development only.
