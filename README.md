# kotatsu(炬燵)

**A sandbox fleet manager and session gateway for AWS Lambda MicroVMs, written in Rust.**

AWS Lambda MicroVMs の上でマルチテナントのサンドボックス製品を作るとき、
すべてのチームが自前で書き直している「真ん中の層」を製品化するプロジェクト。

- ウォームプール(microVM を温かく保つ)
- テナント → microVM のセッション固定ルーティング(endpoint-per-VM の前に立つゲートウェイ)
- JWE トークンの発行・最小スコープ化・自動ローテーション(≤60 分 TTL を隠蔽)
- アイドル suspend / トラフィック resume の調停(resume 中のリクエスト待受)
- メンテナンスリーパ(max-age→terminate、Zombie VM の回収)。idle→suspend は VM 側の `IdlePolicy` が担います
- `kotatsu dev`: AWS 契約をローカルでエミュレートする開発モード

コンセプトと調査結果は **[CONCEPT.md](./CONCEPT.md)**、
1 時間のインターネット調査ログは **[docs/research.md](./docs/research.md)** を参照。

## Crates

| crate | role |
|---|---|
| `kotatsu` | core library — ControlPlane trait、AWS 実装、warm pool、token vending、endpoint client、waiter、cost/metrics、Memory/SQLite store |
| `kotatsud` | セッションゲートウェイデーモン(HTTP/WS プロキシ・Bearer 認証・/metrics) |
| `kotatsu-dev` | ローカル契約エミュレータ(ライフサイクルフック駆動・プロキシ契約・制御 API) |
| `kotatsu` (CLI, `crates/kotatsu-cli`) | 管理 CLI(`vm`/`token`/`image`/`tag`/`dev`/`cost`/`serve`) |

## Quickstart(ローカル、AWS 不要)

```console
# 1. ローカルアプリの前に契約エミュレータを立てる
$ kotatsu dev --app-url http://127.0.0.1:3000 --app-port 3000 --listen 127.0.0.1:4000
emulator endpoint: http://127.0.0.1:4000

# 2. ゲートウェイを mock モードで起動し、エミュレータを指す
#    --app-port は dev 側と揃える(X-aws-proxy-port の受理値)
$ kotatsud --mock --mock-endpoint http://127.0.0.1:4000 \
    --listen 127.0.0.1:9000 --api-key devkey --image dev-image \
    --app-port 3000

# 3. テナント経由でアプリに到達
$ curl -H 'Authorization: Bearer devkey' http://127.0.0.1:9000/t/alice/
```

## CLI

```console
kotatsu vm list [--image ARN] [--version MAJOR.MINOR]
kotatsu vm get MICROVM_ID
kotatsu vm run --image ARN [--ingress-connector SHELL_INGRESS]
               [--idle-suspend-seconds N] [--wait]
kotatsu vm suspend|resume|terminate MICROVM_ID
kotatsu token mint MICROVM_ID --ports 8080[,9000-9010|all] --ttl-minutes N
kotatsu token shell MICROVM_ID
kotatsu image create --name N --s3-uri s3://bucket/img.zip \
                     --base-image-arn ARN --build-role-arn ARN   # 必須
kotatsu image list [--name-filter X]
kotatsu image versions --image ARN        # 各 version の state/status
kotatsu image base [--image BASE_ARN]     # AWS 管理ベースイメージ/そのバージョン
kotatsu image builds --image ARN --version V | build --image ARN --version V --build-id ID
kotatsu image get --image ARN | get-version --image ARN --version V
kotatsu image update --image ARN --s3-uri s3://… --base-image-arn ARN --build-role-arn ARN
kotatsu image update-version --image ARN --version V --status ACTIVE|INACTIVE
kotatsu image delete-version --image ARN --version V   # その version のみ削除(不可逆)
kotatsu image delete --image ARN          # 全バージョンを削除(不可逆)
kotatsu tag list|set|unset --resource ARN [--tag K=V …] [--key K …]
kotatsu dev  --app-url URL [--app-port P] [--listen ADDR]
kotatsu cost --baseline-gb 2 --baseline-seconds N …   # オフライン見積り
kotatsu serve -- <kotatsud への引数>                    # exec 委譲
```

グローバル `--region`(または `AWS_REGION`)でリージョンを固定できます。
トークン値は要求したときだけ stdout に出ます(ログには出ません)。

## kotatsud

```console
KOTATSU_API_KEYS="$OPERATOR_KEY" \
kotatsud --listen 0.0.0.0:9000 \
         --image arn:aws:lambda:…:microvm-image:my-sandbox \
         --warm-size 4 --max-vms 200 \
         --warm-schedule 09:00-18:00=16 --warm-schedule 22:00-06:00=2 \
         --idle-suspend-secs 300 --suspended-ttl-secs 28800 \
         --state-db /var/lib/kotatsu/bindings.db
```

- `/t/{tenant}`、`/t/{tenant}/`、`/t/{tenant}/{*path}`(全メソッド) — tenant affinity で VM を引き当て、`X-aws-proxy-auth`/`X-aws-proxy-port` を注入してプロキシ(HTTP + WebSocket)
- 認証は `Authorization: Bearer` またはブラウザ WS 用 `?key=`(upstream には流れません)。`--api-key` は全 tenant に届く管理者キー、`--tenant-key TENANT=KEY` はその tenant のみに有効なスコープ付きキーです(他 tenant は 403)
- `GET /healthz` / `GET /metrics`(無認証 — プライベート bind または前面で保護してください)
- 中断しても SQLite に binding が残り、再起動後も同じ tenant → VM に戻ります
- `--mock --mock-endpoint URL` で実 AWS なしの開発ができます
- TOML 設定ファイル(`--config`)対応、未知フィールドは拒否。キーの一覧と例は [crates/kotatsud/README.md](crates/kotatsud/README.md#config-file) にあります
- `--warm-schedule HH:MM-HH:MM=N`(UTC・繰り返し可・`24:00` 終端可・`22:00-06:00` で日跨ぎ)で時間帯別の warm サイズ。縮退時は超過 warm VM を terminate します

## セキュリティモデル

- kotatsud 自体は平文 HTTP です。**公開 bind では必ず TLS 終端(LB/リバースプロキシ)を前段に置いてください**。loopback 以外への bind では起動時に警告を出します
- API キーには長いランダムな値(例: `openssl rand -hex 32`)を使い、コマンドライン引数ではなく `KOTATSU_API_KEYS`・`KOTATSU_TENANT_KEYS` か `--config` のファイルで渡してください。引数は同じホストのほかのユーザーが `ps` で読めます
- クライアントの `Authorization`・`?key=`・`Connection` 指名・hop-by-hop ヘッダは upstream に流しません
- tenant 境界: `--tenant-key` のスコープ付きキーは自 tenant の `/t/*` にしか届きません。上流の `Set-Cookie` は `Path` を `/t/{tenant}` 以下に書き換え `Domain` を除去するので、Cookie が tenant をまたぎません(Path を無視する非ブラウザクライアントには効きません — その場合は tenant ごとの host を使ってください)
- `__Host-` で始まる Cookie は `Path=/` が必須なので、`Path` を書き換えた後はブラウザが保存しません。tenant のアプリでは `__Host-` 接頭辞を使わないでください(`__Secure-` は使えます)
- 全 tenant が同じ origin を共有するので、origin 全体に効く上流の応答ヘッダ(`Strict-Transport-Security`・`Alt-Svc`・`Service-Worker-Allowed`・`Clear-Site-Data`・`NEL`・`Report-To`)はクライアントに返しません
- ブラウザ WS 用 `?key=` は TLS 前段の access log や APM に残り得ます。短命のスコープ付きキーを使うか、前段で query を記録しない設定にしてください
- `x-forwarded-for` はクライアントの ConnectInfo から、`x-forwarded-proto` は `--forwarded-proto` 設定値からゲートウェイが生成します
- `AuthToken` は Debug 出力で `<redacted>`、TTL ≤60 分(既定 30 分)・ポートスコープ付きで最小化します
- `--allow-unauthenticated` は loopback bind または `--mock` のときしか起動できません(公開 bind + 実 AWS + 無認証は起動を拒否)
- `kotatsu dev` の `/_kotatsu/*` 制御 API は無認証の dev 用です。gateway 連鎖経由ではテナント認証で到達可能になるので、本番エンドポイントとしては露出しないでください
- `/metrics` は無認証なので必ず private bind か認証付き front-door を置いてください

## Install

GitHub Releases の tarball(プラットフォーム別 `kotatsu` + `kotatsud`)を使うか、ソースから:

```console
cargo install --locked --path crates/kotatsu-cli   # `kotatsu` コマンド
cargo install --locked --path crates/kotatsud      # `kotatsud` ゲートウェイデーモン
```

crates.io 公開後は `cargo install kotatsu-cli` / `cargo install kotatsud` で入り、
ライブラリとしては `cargo add kotatsu` で組み込めます。
crates.io への公開は依存順に `kotatsu` → `kotatsu-dev` → `kotatsud` → `kotatsu-cli` の順で行います
(後続クレートの `cargo publish` は先行クレートが index に現れてから)。

## Build & test

```console
cargo build --workspace
cargo test  --workspace --all-features
cargo run   --example warm_pool -p kotatsu   # Mock 上の warm pool デモ
```

## Docker

```console
docker build -t kotatsu .
export KOTATSU_API_KEYS="$KEY"   # -e に値を書かず、この環境変数を渡す
docker run -p 9000:9000 -e KOTATSU_API_KEYS -v kotatsu-data:/var/lib/kotatsu kotatsu \
    --listen 0.0.0.0:9000 --image arn:…
docker run --entrypoint kotatsu kotatsu vm list   # CLI も同梱
```

## Status

v0.1 — core・gateway・emulator・CLI が実装済み。AWS 実契約の統合検証(実アカウントでの run/suspend/resume 計測)はロードマップ(CONCEPT.md §8)の範囲です。

## License

Apache-2.0 OR MIT。
