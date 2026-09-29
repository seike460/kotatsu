# kotatsu(炬燵)— AWS Lambda MicroVMs サンドボックス・フリート制御 & セッションゲートウェイ

> microVM を温かく保つ。Put your microVMs under the kotatsu.

## 1. 一言でいうと

AWS Lambda MicroVMs 上でマルチテナントのサンドボックス製品(AI コーディング基盤・
インタラクティブ開発環境・ノートブック・脆弱性スキャナ等)を作るとき、
**全チームが自前で書き直している「真ん中の層」を、Rust 製の単一バイナリ +
組込みクレートとして製品化する OSS**。

- ウォームプール(常時温めた microVM の在庫)
- テナント → microVM のセッション固定ルーティング(1 本の公開エンドポイントの後ろに N 台)
- JWE トークンの発行・スコープ絞り・自動更新(60 分上限を利用者に見せない)
- アイドル suspend は AWS `idlePolicy` に委譲。トラフィック resume は
  組込み側が `resume-microvm` を発行しつつ「resume 中のリクエスト待受」
  (waiter)を担う
- ライフサイクル管理(max-age→terminate のリーパ、8h 上限の手前で確実に回収)
- ローカル開発モード(AWS 契約をエミュレートしてフック実装を高速に試す)

## 2. 背景(調査サマリ、2026-09-26 時点)

**AWS Lambda MicroVMs** は 2026 年 6 月 GA の新サーバーレスプリミティブ。
Firecracker ベースの分離実行環境で、API バージョン `2025-09-09`、サービス名 `lambda-microvms`。
リージョンは us-east-1 / us-east-2 / us-west-2 / **ap-northeast-1** / eu-west-1。
**arm64 (aarch64) のみ**。

| 事実 | 意味 |
|---|---|
| microVM ごとに専用エンドポイント `https://<id>.lambda-microvm.<region>.on.aws` | **エンドポイント間の LB は無い**。自分で振り分けが必要 |
| 全リクエストに `X-aws-proxy-auth`(JWE)必須、TTL 最大 60 分 | トークン発行・更新の管理が要る |
| 宛先ポートは `X-aws-proxy-port` ヘッダで指定、WS は subprotocol で運ぶ | プロキシ契約が AWS 独自。実装が散らばる |
| ライフサイクルフック(`/ready` `/validate` `/run` `/resume` `/suspend` `/terminate`)はアプリ側の HTTP エンドポイント | フック実装の DX が品質を左右する |
| idlePolicy + suspend/resume、suspend 中はコンピュート課金なし、最長 8 時間 | 「使わない間は畳む」設計がコストを支配 |
| イメージ = Dockerfile zip(S3)+ 管理ベースイメージ → AWS 側でスナップショットビルド | ビルドが遠隔・非同期。ローカル試行手段が無い |

## 3. なぜ今・なぜこの隙間か

### ユースケース調査の結論

Lambda MicroVMs の想定顧客(AI コーディングエージェント、ブラウザ IDE、Jupyter、
スキャナ、ゲームサーバ、RL 環境、マルチテナント CI)は**全部「テナント/セッション単位で
専用環境を張り、それにトラフィックを届ける」パターン**。つまり本番化には必ず次が要る。

1. N 台の microVM を束ねるプール(起動は速いがゼロ待ちではない。温めると体感が変わる)
2. 「このテナントはこの VM」への固定ルーティング(AWS は endpoint-per-VM、LB を提供しない)
3. 短命 JWE の発行所(ブラウザはヘッダを自由に付けられず、WS は subprotocol 運び)
4. suspend/resume のオーケストレーション(resume 中にリクエストを落とさない)
5. 8 時間上限・Zombie VM の回収(コスト暴走防止)

### 実際、誰もが自作している(調査で確認)

- AWS 公式 Colyseus サンプルは `/_mm` マッチメイクプロキシ + トークン発行所を自作
- aws-samples の Claude エージェント構成は独自オーケストレータを構築
- agent-sandbox-os(asbox)は infra プロビジョナまで自前実装

### 既存 OSS との差別化(被っていないことの確認)

| OSS | 言語 | 領域 | kotatsu との関係 |
|---|---|---|---|
| `aws-sdk-lambdamicrovms` | Rust | 生の生成 SDK(1 API = 1 builder) | 下位依存として利用 |
| `theagenticguy/microvms-agentd` | Rust | **VM 単体**: in-VM デーモン(exec/fs)+ CLI + コスト + conformance | 層が違う。kotatsu は **フリート間**。VM 側 exec は任せられる |
| `dhanababum/agent-sandbox-os`(asbox) | Python | SDK + CLI + MCP + guest agent | Python 製・制御面は非本番寄り。kotatsu は本番常駐インフラ |
| `lambda-microvm-hook-server` | Rust | 特定エージェント(aether)向けフックサーバ | 単機能。kotatsu dev のフックエミュレートとは別物 |
| `kanutocd/lambda-microvms` | Ruby | 実験的 dev kit | 領域外 |
| `aws/agent-toolkit-for-aws` | — | AI エージェント用スキル定義 | 補完(利用者側の道具) |

**結論: 「VM 1 台をどう使うか」は埋まった。「N 台の前に立つ制御面」が空白。**
そこが kotatsu のポジション。

## 4. コンセプト

炬燵は「座れば暖かい場所を予め用意し、離れても温もりが残る」装置。
kotatsu は「テナントが来たら温かい microVM がすぐ用意され、離れたら畳んで置かれる」
制御面。ネーミングの芯は **warm pool**。

### 構成物(Rust workspace)

| crate / bin | 役割 |
|---|---|
| `kotatsu` | 組込みコアライブラリ。`SandboxPool` / `Sandbox` / `MicrovmEndpoint` / `TokenVending` を型付きで提供。自分のサービスに直接組める |
| `kotatsud` | 常駐ゲートウェイデーモン。単一の HTTP エンドポイントとして立ち(TLS は前段で終端)、テナント → microVM を解決して中継 |
| `kotatsu`(CLI) | `vm` / `token` / `image` / `tag` / `dev` / `cost` / `serve` サブコマンド。warm pool の操作面は `kotatsud` に内包される(CLI からは `serve` で起動するだけ) |
| `kotatsu-dev` | ローカルエミュレーションモード(下記) |

### kotatsud(ゲートウェイ)がやること

1. クライアントは `https://kotatsu.example/t/{tenant}/...` など自前認証
   (Bearer API キー、`--tenant-key` で tenant スコープ可)で来る
2. tenant → microVM を解決。無ければ `run-microvm`、SUSPENDED なら `resume-microvm` を
   発行し、**RUNNING になるまでリクエストを保持**(waiter で初撃を落とさない)
3. その VM 専用の JWE を mint(TTL<60min で自動ローテ、ports は必要最小スコープ)
4. `X-aws-proxy-auth` / `X-aws-proxy-port` を注入して VM endpoint へ中継
   (HTTP/1.1 + WebSocket。SSE は単なるレスポンスストリームとして動作。
   gRPC はトレーラー透過が未対応のため現状対象外)
5. アイドル suspend は VM 側 `idlePolicy` が担い、ゲートウェイは
   max-age 到来 → `terminate-microvm` と warm 補充を担う
6. Prometheus メトリクス(`/metrics`)と tracing ログで観測可能に。コスト見積りは
   CLI の `kotatsu cost` とライブラリの `kotatsu::cost` が担う
   (組込みの料金定数は us-east-1・ARM の公式単価)

### kotatsu core(組込みライブラリ)実際の API

```rust
let cp: Arc<dyn ControlPlane> = Arc::new(AwsControlPlane::new(&aws_config));
let mut run = RunRequest::new("arn:aws:lambda:ap-northeast-1:123456789012:microvm-image:code-sandbox");
run.idle_policy = Some(IdlePolicyConfig {
    auto_resume_enabled: true,
    max_idle_duration_seconds: 15 * 60,
    suspended_duration_seconds: 28_800,
});
let mut cfg = PoolConfig::new(run);
cfg.warm_size = 4;
cfg.max_vms = 200;
cfg.max_age = Some(Duration::from_secs(6 * 3600));
let pool = SandboxPool::new(cp, Arc::new(SqliteStore::open("state.db").await?), cfg)?;

let sandbox = pool.acquire(&TenantKey::new("user-42")?).await?; // run/resume を内包
let req = sandbox.endpoint().post("/exec").await?.json(&body); // X-aws-proxy-auth 注入済み
sandbox.release().await?; // または suspend() — プール方針と tenant バインディングに従う
```

### kotatsu dev(ローカル開発モード)

イメージビルドは AWS 側の非同期処理で、フック実装の試行錯誤が遅い。
`kotatsu dev` は **AWS 側の契約だけをローカルで再現**する。

- アプリは利用者が手元で(直接、または Docker/Finch/Apple container などで)起動しておき、
  `--app-url` で指す。`kotatsu dev` はアプリもコンテナも起動しない
- エミュレートする契約: `/validate` `/run` `/ready` `/suspend` `/resume` `/terminate` フック呼出、
  `X-aws-proxy-auth` / `X-aws-proxy-port` / WS subprotocol、PENDING→RUNNING、SUSPENDED→RUNNING の遷移
- 本物の VMM(libkrun/Firecracker)での実行はスコープ外(契約の再現に集中)

## 5. 機能ロードマップ

- **v0.1**(実装済): `run/get/suspend/resume/terminate` ラッパ + tenant→VM テーブル +
  JWE mint/refresh + HTTP プロキシ + max-age リーパ + `kotatsu dev`、
  WS 通過(SSE はプロキシのストリーミング経路で通過。
  gRPC はトレーラー透過未対応のため現状対象外)、
  warm pool サイジング(定数 + `WarmWindow` による UTC 時間帯スケジュール、
  縮退時は超過 VM を terminate)、Prometheus、コスト見積、SQLite 状態ストア(再起動耐性)
- **next**: 複数ゲートウェイの状態共有(DynamoDB/ElastiCache Serverless 等、要検証)、
  per-tenant クォータ・レート制限、トークン narrower-scope ポリシー
- **later**: OTel トレース、MCP サーバ、`microvms-agentd` イメージとの連携プリセット

## 6. なぜ Rust か

- 常駐ゲートウェイは「N 並行の WS/SSE を低メモリで捌く」仕事。Rust の実需が素直に効く
- 単一バイナリ(Linux 版は glibc 2.28 以上)で Lambda / Fargate / EC2 / **microVM 内** のどこへも置ける
- `aws-sdk-lambdamicrovms` が GA 済み。生成 SDK への薄い高級ラッパとして
  typestate(「RUNNING でしか connect できない」等を型で閉じる)の旨味が出せる
- エコシステムに Rust 製の「フリート層」が無い(microvms-agentd は単体層)

## 7. 名前の選定

和名(日本語由来)で、製品の意味に合う名前として kotatsu(炬燵)を選んだ。
炬燵は warm pool のメタファー(§4)。ほかの候補と落選理由は `docs/research.md` の
「ネーミング調査」にある。

## 8. リスク・未検証事項

- `run-microvm`/同時実行数のアカウントクォータ(プールの上限設計に直結。要実測)
- resume レイテンシの実測分布(「保持して待つ」方式の体感に直結)
- JWE の Rotation 粒度・同時有効本数
- WS の長時間維持に対するサービス側タイムアウト
- VPC エンドポイント経由接続(`com.amazonaws.<region>.lambda-microvm`)での挙動差
- イメージビルド時間の実測(dev モードの価値づけ)
- microvms-agentd が将来フリート層を内包する可能性(差別化を warm/gateway/dev に絞る)

## 9. ライセンス

`MIT OR Apache-2.0`(Rust エコシステムで一般的なデュアルライセンス)。
条文はリポジトリ直下の `LICENSE-MIT` と `LICENSE-APACHE`。

## 10. 参考(主要ソース)

- AWS Lambda MicroVMs 製品/ドキュメント: https://aws.amazon.com/lambda/lambda-microvms/
- API Reference(2025-09-09): https://docs.aws.amazon.com/lambda/latest/microvm-api/
- ライフサイクル/フック: https://docs.aws.amazon.com/lambda/latest/dg/microvms-how-it-works.html
- ネットワーキング契約: https://docs.aws.amazon.com/lambda/latest/dg/microvms-networking.html
- aws/agent-toolkit-for-aws(skill): https://github.com/aws/agent-toolkit-for-aws
- theagenticguy/microvms-agentd: https://github.com/theagenticguy/microvms-agentd
- dhanababum/agent-sandbox-os: https://github.com/dhanababum/agent-sandbox-os
- aws-samples(Claude agents / Colyseus): github.com/aws-samples

調査ログ全文は `docs/research.md`。
