# リリース手順

メンテナ向けの手順です。`X.Y.Z` は新しい版、`P.Q.R` は直前の版を表します。

## 1. version を上げる

次の 6 か所を `X.Y.Z` にします。

- `crates/kotatsu/Cargo.toml`・`crates/kotatsu-dev/Cargo.toml`・
  `crates/kotatsud/Cargo.toml`・`crates/kotatsu-cli/Cargo.toml` の `version`
- ルートの `Cargo.toml` の `[workspace.dependencies]` にある、
  `kotatsu` と `kotatsu-dev` の `version`

続けて `cargo check --workspace` を実行し、更新された `Cargo.lock` もコミットします。
CI は `--locked` で動くので、`Cargo.lock` が古いままだと失敗します。

## 2. CHANGELOG.md を更新する

- `## [Unreleased]` の下に `## [X.Y.Z] - YYYY-MM-DD` を足し、未リリースの項目をそこへ移します
- 末尾の `[Unreleased]` のリンクを `compare/vX.Y.Z...HEAD` に直します
- `[X.Y.Z]: https://github.com/seike460/kotatsu/compare/vP.Q.R...vX.Y.Z` を足します

## 3. 公開の前に確かめる

- main の CI が通っていること(release commit を含む PR を merge した後の main)
- `cargo publish --workspace --dry-run --locked` が通ること
- `release.yml` を変えた版では、Actions の `release` を main で手動実行します(`workflow_dispatch`)。
  3 つのビルドと glibc の検査が通ることを確かめます。手動実行ではビルドとパッケージまでを行い、Release は作りません

## 4. crates.io に公開する

`README.md` と `crates/kotatsu-cli/README.md` は、crates.io から入れる手順を案内しています。
merge の後はこの手順まで続けて行い、GitHub Release より先に crate を公開します。
crates.io の公開は取り消せません(yank しかできません)。

```console
cargo publish --workspace --locked
```

cargo が依存順(`kotatsu` → `kotatsu-dev` → `kotatsud`・`kotatsu-cli`)に公開します。
依存先の crate が index に現れるのを待ってから、次の crate を公開します。
この公開はまとめて成功するとは限りません。途中で失敗したら、残りの crate を `-p` で指定して公開し直します。

## 5. tag を push する

crates.io の公開が済んでから tag を push します。Release が見えた時点で、案内どおりに `cargo install` できるようにするためです。

```console
git tag -s vX.Y.Z -m "kotatsu vX.Y.Z"
git push origin vX.Y.Z
```

`release.yml` が tag と `kotatsu-cli`・`kotatsud` の version を照合し、テストを通してから、
各プラットフォームの tarball と checksum を GitHub Release に載せます。
tag に `-` が入る版(`v0.2.0-rc.1` など)は pre-release になります。
Release の本文は GitHub の自動生成ノートになるので、CHANGELOG の `[X.Y.Z]` 節に差し替えます。
Release のビルドが失敗したら、原因を直してから workflow を再実行します。コードの修正が要るときは、
crates.io の版と食い違わないように、次の patch 版として出し直します。
