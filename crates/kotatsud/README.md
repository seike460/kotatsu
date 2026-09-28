# kotatsud

Session gateway daemon for AWS Lambda MicroVM sandboxes — one HTTP
endpoint that resolves `tenant → microVM`, mints scoped
`X-aws-proxy-auth` tokens, and proxies HTTP/WebSocket traffic to the
per-VM endpoint.

```console
KOTATSU_API_KEYS="$KEY" kotatsud --listen 0.0.0.0:9000 --image arn:… --state-db state.db
```

kotatsud serves plain HTTP and does not terminate TLS. On a public
bind, put a TLS-terminating load balancer or reverse proxy in front of
it, or API keys and tenant traffic cross the network in clear text.
Use long random keys (e.g. `openssl rand -hex 32`), and pass them
through `KOTATSU_API_KEYS` / `KOTATSU_TENANT_KEYS` or the config file
rather than `--api-key` / `--tenant-key`: other users on the host can
read a process's command-line arguments.

`kotatsud --help` lists every flag with its environment variable.
`GET /healthz` and `GET /metrics` (Prometheus) share the listen socket
without authentication. The metric names are documented in
`kotatsu::metrics::names`.

## Config file

`--config FILE` (or `KOTATSU_CONFIG`) reads a TOML file, and unknown
keys fail startup. Flags and environment variables take precedence
over the file; for `api_keys` and `tenant_keys` a flag replaces the
whole list. Durations are strings such as `"90s"`, `"5m"` or `"8h"`,
while the matching flags take whole seconds.

| key | flag | value | default |
|---|---|---|---|
| `listen` | `--listen` | socket address | `127.0.0.1:3000` |
| `region` | `--region` | AWS region | AWS SDK default chain |
| `image` | `--image` | image ARN or ID | required unless `--mock` |
| `app_port` | `--app-port` | port | `8080` |
| `warm_size` | `--warm-size` | integer | `4` |
| `warm_schedule` | `--warm-schedule` | array of `"HH:MM-HH:MM=N"` (UTC) | none |
| `max_vms` | `--max-vms` | integer | `100` |
| `max_age` | `--max-age-secs` | duration, `"0s"` disables | off |
| `idle_suspend` | `--idle-suspend-secs` | duration, `"0s"` (off) or 60s–8h | off |
| `suspended_ttl` | `--suspended-ttl-secs` | duration, 0–8h | `8h` |
| `wait_timeout` | `--wait-timeout-secs` | duration | `2m` |
| `maintenance_interval` | `--maintenance-secs` | duration | `60s` |
| `forwarded_proto` | `--forwarded-proto` | string | `"http"` |
| `reap_lost_vms` | `--reap-lost-vms` | bool | `false` |
| `state_db` | `--state-db` | path | `$XDG_DATA_HOME/kotatsu/bindings.db` (else `~/.local/share/…`) |
| `api_keys` | `--api-key` | array of keys | none |
| `tenant_keys` | `--tenant-key` | array of `"TENANT=KEY"` | none |
| `allow_unauthenticated` | `--allow-unauthenticated` | bool | `false` |

`--mock` and `--mock-endpoint` exist only as flags. The file holds API
keys, so keep it readable by the service user only.

```toml
listen = "0.0.0.0:9000"
image = "arn:aws:lambda:…:microvm-image:my-sandbox"
warm_size = 4
max_vms = 200
warm_schedule = ["09:00-18:00=16", "22:00-06:00=2"]
idle_suspend = "5m"
suspended_ttl = "8h"
wait_timeout = "30s"
forwarded_proto = "https"
state_db = "/var/lib/kotatsu/bindings.db"
tenant_keys = ["alice=REPLACE_WITH_A_RANDOM_KEY"]
```

License: Apache-2.0 OR MIT.
