# node-tls-behind-proxy

Connects spark-connect-js to a Spark Connect server over TLS by terminating TLS at a Caddy reverse proxy. Spark Connect 4.0 has no native server-side TLS; production deployments terminate at a proxy in front of port 15002.

## Run

```sh
pnpm gen-cert       # mint a self-signed cert in ./certs (demo only)
pnpm spark:up
pnpm build && pnpm start
pnpm spark:down
```

Connects to `sc://localhost:8443/;use_ssl=true` by default; override with `SPARK_REMOTE`. Requires Node.js 22+, Docker, and openssl.

`pnpm start` sets `NODE_EXTRA_CA_CERTS=./certs/cert.pem` so Node trusts the demo cert. With a cert that chains to a public CA, drop both the env var and the cert generation step.
