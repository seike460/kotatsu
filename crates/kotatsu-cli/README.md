# kotatsu-cli

The `kotatsu` operational CLI for AWS Lambda MicroVMs — `vm`
(list/get/run/suspend/resume/terminate), `token` (mint/shell), `image`
and `tag` management, `cost` estimates, `dev` (local emulator), and
`serve` (runs the kotatsud gateway).

`serve` execs the `kotatsud` binary from `PATH`, which this crate does
not install. Take both binaries from a
[GitHub Releases](https://github.com/seike460/kotatsu/releases) tarball,
or build them from the repository:

```console
cargo install --locked --git https://github.com/seike460/kotatsu kotatsu-cli kotatsud
kotatsu vm list
```

They are also on crates.io:
`cargo install --locked kotatsu-cli kotatsud`.

See the [repository](https://github.com/seike460/kotatsu).

License: Apache-2.0 OR MIT.
