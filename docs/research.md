# 調査ログ — AWS Lambda MicroVMs / Rust OSS ギャップ分析

調査日: 2026-09-26 / 目的: 「AWS Lambda MicroVMs を使う上で便利な Rust OSS」の
ユースケース特定と既存 OSS との差別化。

## サービス仕様(確認済み事実)

### 基本

- 発表: AWS News Blog「Run isolated sandboxes with full lifecycle control:
  AWS Lambda introduces MicroVMs」/ What's New 2026-06。
- 位置づけ: 「serverless compute primitive」。Firecracker 製、15 兆/月の
  Lambda 呼び出しと同じ土台。ユーザー/AI 生成コードを隔離実行するサンドボックス。
- リージョン: us-east-1, us-east-2, us-west-2, ap-northeast-1, eu-west-1。
- アーキテクチャ: **arm64 のみ**(agentd が aarch64-musl でクロスコンパイルする理由)。
- アプリは **microVM 内のコンテナ**として動く(AL2023 上)。Dockerfile を zip→S3→
  `create-microvm-image` → AWS 側が OCI 化し ENTRYPOINT/CMD 起動→`/ready` で
  待機→**メモリ+ディスクのスナップショット**を採取。以後の起動はスナップショットから復帰。
- ベースイメージ: 管理イメージ `al2023-1`(バージョン管理・deprecation ライフサイクル有)。
- サイジング: baseline 0.5GB/0.25vCPU〜8GB/4vCPU、ピーク時 4x まで垂直スケール、秒課金。
- 寿命: `maximumDurationInSeconds` 最大 28,800(8h)。suspend 中はコンピュート課金なし。
  `suspendedDurationSeconds` 超過 or 明示 terminate で終了。

### API(`lambda-microvms`, 2025-09-09)24 オペレーション

CreateMicrovmAuthToken / CreateMicrovmImage / CreateMicrovmShellAuthToken /
DeleteMicrovmImage / DeleteMicrovmImageVersion / GetMicrovm / GetMicrovmImage /
GetMicrovmImageBuild / GetMicrovmImageVersion / ListManagedMicrovmImages /
ListManagedMicrovmImageVersions / ListMicrovmImageBuilds / ListMicrovmImages /
ListMicrovmImageVersions / ListMicrovms / ListTags / ResumeMicrovm / RunMicrovm /
SuspendMicrovm / TagResource / TerminateMicrovm / UntagResource /
UpdateMicrovmImage / UpdateMicrovmImageVersion

### エンドポイント/認証契約

- endpoint: `https://<microvm-id>.lambda-microvm.<region>.on.aws`(VM ごとに一意)。
- **エンドポイント間 LB なし**:「There is no load-balancing across MicroVMs from a
  single endpoint – each endpoint is linked to a single MicroVM」= 公式ドキュメント明記。
- 認証: `X-aws-proxy-auth` ヘッダに JWE(`create-microvm-auth-token`、TTL 最大 60 分、
  `allowedPorts` で `{port}`/`{range}`/`{allPorts}` スコープ必須)。
- 宛先ポート: `X-aws-proxy-port`(既定 8080)。
- WebSocket: subprotocol で運ぶ(`lambda-microvms` /
  `lambda-microvms.authentication.<token>` / `lambda-microvms.port.<n>`)。
  ブラウザは WS に任意ヘッダを付けられないため。AWS 側で subprotocol は剥がして転送。
- HTTP/2・gRPC・SSE・WebSocket 対応。
- Shell: `create-microvm-shell-auth-token` + `SHELL_INGRESS` コネクタ →
  コンソール or WS 端末でコンテナ内シェル。
- VPC 接続: `com.amazonaws.<region>.lambda-microvm` の interface endpoint 経由可。
- Network connector: `ALL_INGRESS` / `INTERNET_EGRESS` / `SHELL_INGRESS` /
  自作 VPC egress。run 時に指定し実行中は変更不可。

### ライフサイクルフック(アプリ側が出す HTTP エンドポイント)

- Image build 用: `/ready`(/aws/lambda-microvms/runtime/v1/ready、503=継続待ち
  /200=スナップショット開始、1–3600s)、`/validate`(ビルド後の検証実行。
  モック payload を流すと snapshot のホット領域最適化にも効く)
- Runtime 用: `/run`(runHookPayload ≤16KB + microvmId が body で届く)、
  `/resume`、`/suspend`、`/terminate`。いずれも hooks.port で指定したポートに
  Lambda が HTTP 呼出。`/run` の失敗/timeout で RUNNING を経ず TERMINATING に
  直行しうる。auto-resume(`/resume` フックを含む)が失敗すると呼び出し元に 502。
- State: PENDING→RUNNING→SUSPENDING→SUSPENDED→RUNNING→TERMINATING→TERMINATED。
- 注意: ビルド時に生成した一意な値(ID/シークレット/接続)は同一イメージ全 VM で
  共有される → `/run` で生成する設計が必須。

## 既存 OSS / ツール棚卸し

| 名前 | 言語 | 内容 | 評価 |
|---|---|---|---|
| aws-sdk-lambdamicrovms | Rust | 生成 SDK(v1.4.0、2026-06 初出、DL ~30k) | 生 API のみ。高級機能なし |
| **theagenticguy/microvms-agentd** | Rust | agentd(in-VM daemon、aarch64-musl)+ microvm CLI + Rust/Python/Node クライアント。exec・ファイル転送・コストレポート・conformance suite・stateright 検証 | **最強の先行**。層=VM 単体。フリート/ルーティングは扱わない |
| dhanababum/agent-sandbox-os | Python | asbox: SDK + asb CLI + agentd(FastAPI:8080 + hook server:9000)+ boto3 infra + MCP | オールインワンだが Python・実験寄り |
| lambda-microvm-hook-server | Rust | aether エージェント用 hook サーバ | 単機能 |
| kanutocd/lambda-microvms | Ruby | 実験 dev kit(sdk-contract 検査等) | 実験段階 |
| aws/agent-toolkit-for-aws | — | aws-lambda-microvms SKILL.md(AI エージェント用) | 補完関係 |
| aws-samples | Python/TS | Claude managed agents 構成、Colyseus ゲーム鯖構成 | **どちらも matchmake proxy + token 発行を自作している** = 需要の証拠 |
| arunksingh16/aws-lambda-microvm | Python | Bedrock 連携の小さな POC | 参考 |

## ギャップ分析 → 採用コンセプト

空白 = **フリート制御 + セッションゲートウェイ**(理由: endpoint-per-VM で LB なし、
JWE 60 分制限、suspend/resume 調停、warm pool 不在。公式サンプルが全て自作している層)。
補助機能 = ローカル契約エミュレーション(`kotatsu dev`)。

不採用だった方向: 
- VM 内 exec/fs エージェント → microvms-agentd が高品質で実施済み
- MCP サーバ単体 → agent-toolkit/asbox が既存
- 生 SDK の別ラッパ → 価値薄い

## ネーミング調査

- 要件: 和名・意味が製品と一致・crates.io/GitHub/PyPI で衝突なし
- 落選: sunaba(PyPI で sandbox MCP が同名稼働)、hibana/engawa/okami(crates 登録済)、
  ryokan(同名 Rust OSS が別領域で稼働)、hanabi(crates 登録済)、
  hakoniwa(toppers/hakoniwa が著名)
- 採用: **kotatsu(炬燵)** — warm pool メタファー。crates.io 空き。
- 予備: hatago / yadoya / monban / toride(いずれも空き)

## 主要 URL

- https://aws.amazon.com/blogs/aws/run-isolated-sandboxes-with-full-lifecycle-control-aws-lambda-introduces-microvms/
- https://aws.amazon.com/blogs/compute/announcing-lambda-microvms-serverless-compute-environments-with-vm-level-isolation-and-near-instant-startup/
- https://aws.amazon.com/blogs/compute/secure-code-execution-for-ai-agents-with-aws-lambda-microvms/
- https://docs.aws.amazon.com/lambda/latest/dg/microvms-how-it-works.html
- https://docs.aws.amazon.com/lambda/latest/dg/microvms-images.html
- https://docs.aws.amazon.com/lambda/latest/dg/microvms-launching.html
- https://docs.aws.amazon.com/lambda/latest/dg/microvms-networking.html
- https://docs.aws.amazon.com/lambda/latest/microvm-api/API_RunMicrovm.html
- https://crates.io/crates/aws-sdk-lambdamicrovms
- https://github.com/theagenticguy/microvms-agentd
- https://github.com/dhanababum/agent-sandbox-os
- https://github.com/aws/agent-toolkit-for-aws/tree/main/skills/specialized-skills/serverless-skills/aws-lambda-microvms
- https://github.com/aws-samples/sample-lambda-microvm-claude-managed-agents
- https://github.com/aws-samples/sample-host-colyseus-on-awslambda-microvms

## 料金調査(T5, aws.amazon.com/lambda/pricing "Lambda MicroVMs" 節)

us-east-1 / ARM(Graviton) の公式単価:

- vCPU: $0.0000276944 / vCPU秒
- メモリ: $0.0000036667 / GB秒
- snapshot write(suspend 時): $0.0038 / GB
- snapshot read(resume・launch 時): $0.00155 / GB
- snapshot storage(image + suspend 中の状態): $0.08 / GB月

課金ルール:

- baseline(メモリで指定、2:1 で vCPU 割当)は RUNNING の間ずっと課金、1秒単位
- peak は baseline の 4 倍まで垂直スケール、超過分は「アクティブな時間だけ」課金
- SUSPENDED は compute 無料。suspend 最長 8 時間
- snapshot の write=suspend 毎、read=resume/launch 毎に課金
- MicroVM image の保存は最低 1 週間の retention
- data transfer は標準 AWS 料金(別計算、見積りスコープ外)

ベースライン帯(メモリ/vCPU → peak):
0.5GB/0.25→2GB/1、1GB/0.5→4GB/2、2GB/1→8GB/4、4GB/2→16GB/8、8GB/4→32GB/16

公式 Pricing Example 1(100 開発者の coding sandbox)を `cost` モジュールの
回帰テストとして採用: compute $1,103.38 + snapshot I/O $134.60 +
storage $3.24 = $1,241.22/月
