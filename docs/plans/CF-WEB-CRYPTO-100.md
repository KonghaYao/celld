# CF Workers Web Crypto — 100% 兼容计划

> **目标**：`crypto.subtle` + `crypto` 扩展与 [Cloudflare Workers Web Crypto](https://developers.cloudflare.com/workers/runtime-apis/web-crypto/) 文档矩阵 **逐格对齐**（含 `DigestStream`、`timingSafeEqual`、`MD5`、Secure Curves / `NODE-ED25519`）。
>
> **非目标**：浏览器 Chromium 全量 W3C；`node:crypto` 另立 track（`NODE-CRYPTO-COMPAT.md` 待写）。
>
> **原则**（`celld/docs/testing.md`）：**workerd ↔ celld 差分输出一致**；Rust 原语单轨；禁止第二套 npm polyfill。

---

## 验收定义（100%）

1. **矩阵**：CF 文档「Supported algorithms」表中每个 ✓ 格子，celld 有对应测试用例且 **celld == workerd**（或文档允许的已知差异显式 `expect`）。
2. **方法**：`encrypt/decrypt/sign/verify/digest/generateKey/deriveKey/deriveBits/importKey/exportKey/wrapKey/unwrapKey` + `timingSafeEqual` 全覆盖。
3. **门禁**：`cargo test -p celld`（含 `celld_internal_tests` 时 crypto 套件）；CI 脚本 `scripts/crypto-conformance.sh`（workerd + celld 双跑）。
4. **文档**：`cloudflare-compat.md` Web Crypto 节改为 **Full**，缺口清零或列 `KNOWN_DIVERGENCE`。

当前估算：**~55% 矩阵 / ~80% 常用路径**（2026-09 审计）。

---

## 差距清单（实现 backlog）

按 **Rust 已有 / 仅 JS 接线** 排序：

| ID | 能力 | 现状 | 主要改动 |
|----|------|------|----------|
| C1 | Ed25519 `verify` | Rust `ed25519-verify` | `crypto.js` `verify()` |
| C2 | RSASSA-PKCS1-v1_5 `sign` | Rust `rsa-pkcs1-sign` | `crypto.js` `sign()` |
| C3 | RSA-OAEP `encrypt` | 仅 decrypt | `crypto.rs` + `encrypt()` |
| C4 | RSA-PSS sign/verify | 无 | Rust + JS（对齐 workerd） |
| C5 | `wrapKey` / `unwrapKey` | 无 | JS + AES-KW + RSA-OAEP wrap |
| C6 | AES-KW | 无 | host op 或复用 AES + RFC3394 |
| C7 | ECDSA P-384/P-521 | 主要 P-256 | `crypto.rs` 曲线扩展 |
| C8 | X25519 derive | 部分声明 | `ecdh-derive` / import 对齐 workerd |
| C9 | `NODE-ED25519` 语义 | 部分 | 与 CF 脚注一致（无 raw 私钥 import 等） |
| C10 | 错误类型 / 边界 | 部分 | WPT 子集 + workerd 对照 |

---

## 测试金字塔

```
                    ┌─────────────────────┐
                    │ workerd ∥ celld     │  ← 门禁（真 CF 运行时）
                    │ 同一 fixture 输出   │
                    └──────────┬──────────┘
                               │
              ┌────────────────┴────────────────┐
              │  fixtures/crypto/*.mjs          │  ← 每算法×操作一格
              │  + golden vectors (EdgeEver…)   │
              └────────────────┬────────────────┘
                               │
              ┌────────────────┴────────────────┐
              │  Rust unit: crypto.rs           │  ← pbkdf2/hkdf/aes/rsa 向量
              └─────────────────────────────────┘
```

### Fixture 约定

- 路径：`celld/tests/crypto-conformance/fixtures/<algo>-<op>.mjs`
- 每个 fixture **只 export** `run()` → 可 JSON 序列化的结果（hex/base64），禁止 `Date.now()` 非确定性（除非双端同步种子）。
- Runner：`celld/tests/crypto-conformance/run.mjs` 调 `workerd` 与 `celld`（或 in-process harness），diff 输出。

### 首批 fixture（Phase 0）

- `pbkdf2-deriveBits.mjs` — EdgeEver 参数（100k, SHA-256）
- `hkdf-deriveBits.mjs`
- `ed25519-sign-verify.mjs`
- `rsassa-pkcs1-sign-verify.mjs`
- `rsa-oaep-encrypt-decrypt.mjs`
- `aes-gcm-roundtrip.mjs`
- `wrapKey-aes-kw.mjs`（Phase 2 启用）

---

## 分阶段实施

### Phase 0 — 测试基建 + 快赢（1–2 周）

- [ ] 目录 `tests/crypto-conformance/` + runner 骨架
- [ ] C1、C2、C3 JS 接线
- [ ] 文档更新 compat 表
- [ ] `cargo test` + 首版 conformance 脚本

### Phase 1 — RSA-PSS + RSA-OAEP 满矩阵（2–3 周）

- [ ] C4 移植 workerd `node/internal/crypto` PSS 路径
- [ ] C3 encrypt 与 CF 表对齐
- [ ] fixture 覆盖 RSA 三族

### Phase 2 — wrap/unwrap + AES-KW（2 周）

- [ ] C5、C6
- [ ] 依赖 Phase 1 RSA-OAEP encrypt

### Phase 3 — 曲线与 Secure Curves（2–3 周）

- [ ] C7、C8、C9
- [ ] 从 workerd  port `crypto-keys-test.js` 相关向量

### Phase 4 — WPT 子集 + 发布（持续）

- [ ] 导入 Web Crypto WPT 可运行子集
- [ ] `cloudflare-compat.md` → **Full**
- [ ] cellp：`e2e` 可选 smoke（`nodejs_compat` worker）

---

## Ultracode 工作流映射

| 阶段 | Agent 角色 | 产出 |
|------|------------|------|
| Plan | `plan` | 本文件细化 + fixture 列表 |
| Parallel Review | `explorer`×3 | 安全 / workerd 对照 / 测试覆盖 |
| Implement | `coder` | `crypto.js` + `crypto.rs` PR |
| Fix | `coder` | review 项 |
| Verify | `verification` | 跑 conformance + `cargo test` |

---

## 风险

- **workerd 未安装**：差分门禁降级为 celld golden-only（CI 必须装 workerd）。
- **RSA-PSS / wrap**：工作量大，勿与 `node:crypto` PSS 混轨。
- **submodule**：cellp 需 bump `celld` pointer 后 `go test` / dev 栈回归。

---

## 引用

- `celld/crates/celld/js/crypto.js` / `crypto.rs` / `node_crypto.js`
- `celld/docs/cloudflare-compat.md`
- Cloudflare 算法表（Web Crypto 页）
