# kotatsud

Session gateway daemon for AWS Lambda MicroVM sandboxes — one HTTPS
endpoint that resolves `tenant → microVM`, mints scoped
`X-aws-proxy-auth` tokens, and proxies HTTP/WebSocket traffic to the
per-VM endpoint.

```console
kotatsud --listen 0.0.0.0:9000 --image arn:… --api-key "$KEY" --state-db state.db
```

See the [repository](https://github.com/seike460/kotatsu) for the full
flag reference, TOML config, and metrics endpoints.

License: Apache-2.0 OR MIT.
