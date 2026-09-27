# kotatsu-dev

Local AWS Lambda MicroVMs contract emulator — reproduces the lifecycle
hooks (`/aws/lambda-microvms/runtime/v1/*`), the `X-aws-proxy-auth` /
`X-aws-proxy-port` proxy contract, WebSocket subprotocols, and
PENDING/SUSPENDED state transitions, so gateway and app code can be
tested without an AWS account.

Used by `kotatsu dev`. See the
[repository](https://github.com/seike460/kotatsu).

License: Apache-2.0 OR MIT.
