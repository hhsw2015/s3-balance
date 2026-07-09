# OpenList `s3-balance` Driver 迁移文档 — Review Report

## 综述

四轮独立 review (driver 合规 / R2 协议 / 架构运维 / 安全) 一共发现 **49 个 findings**。合并去重 + 分级后,进入迁移文档 v2 需要处理的关键条目:

- **Critical**: 2
- **High**: 12
- **Medium**: 14
- **Low**: 5

共计 **33 条**要修 (剩余 16 条是重复/低价值)。

---

## Critical 级问题 (必修,否则 v1 直接跑不通)

### C1. PutURL 返回 nil 会污染 dir cache (Review 1)

**验证**: `/Users/wowdd1/Dev/OpenList/internal/op/fs.go:715-760`

```go
switch s := storage.(type) {
case driver.PutURL:
    err = s.PutURL(ctx, dstDir, dstName, url)
default:
    return errors.WithStack(errs.NotImplement)
}
if err == nil {
    // ...
    if newObj == nil {
        newObj = &model.Object{Name: dstName, ..., Mask: model.Temp}
    }
    cache.UpdateObject(newObj.GetName(), newObj)
}
```

**问题**: 我们设计的 PutURL 是**异步**(立即返回 nil,后台跑),`op.PutURL` 认为已完成,插入一个 `Mask: Temp` 的 fake object 到 dir cache。用户 refresh 目录立刻"看到"文件,但字节其实还没到 R2。点下载会 500。

**必须修**: PutURL 内部要么**同步等到 R2 里有对象再返回**,要么**返回 `errs.NotImplement` 让 SimpleHttp 走 tache 路径**。

### C2. SSRF (Review 4)

Worker 接受任意 `source_url`,可以拉 `http://169.254.169.254/` 等内网元数据服务,通过 R2 落地泄漏。

**必须修**: driver 侧对 `source_url` 做 scheme + host 校验 (只允许 `https://`, 拒绝 RFC1918 + 元数据 IP)。Worker 侧再做一层防御 (host 白名单可空,但拒绝 loopback/private/link-local)。

---

## High 级问题 (会导致数据损坏/资金损失/严重体验缺陷)

### H1. AWS SDK v2 checksum trailer 破坏 R2 兼容 (Review 2)

aws-sdk-go-v2 v1.30+ 默认注入 `x-amz-checksum-crc32` header 和 flexible checksum trailers。R2 不认,PUT/UploadPart 会失败。

**必修**: 初始化 S3 client 时:
```go
o.RequestChecksumCalculation = aws.RequestChecksumCalculationWhenRequired
o.ResponseChecksumValidation = aws.ResponseChecksumValidationWhenRequired
```

### H2. `Content-Length: null` 破坏 chunked source (Review 2)

Worker 现在直接从 srcRes.headers 取 Content-Length。source 若是 chunked encoding 就没这 header,PUT 到 R2 会 411。

**必修**: Worker 用 `rangeEnd - rangeStart + 1` 作为 Content-Length,不依赖 source header。

### H3. Copy > 5GB 走 CopyObject 会失败 (Review 2)

R2 CopyObject 硬限 5GB。文档 §7 说"直接 CopyObject"。

**必修**: Copy 判断 srcObj.size,>5GB 时走 UploadPartCopy 或返回 NotSupport 落到 fallback。

### H4. Init 无 credential 校验 (Review 3)

Init 只解析 JSON,错的 AK/SK 首次上传/下载才暴露,storage 一直显示 "healthy"。

**必修**: Init 里对每个 bucket 做 `HeadBucket`,失败标记 storage status=disconnect。

### H5. Drop 无 in-flight 任务清理 (Review 3)

Drop 时 tache goroutines 还在跑,SQLite 被关会 panic,R2 multipart upload 永久 orphan。

**必修**: Drop 里 cancel tache manager 的 ctx,等 goroutines 退出,对 status=running 的 offline_tasks 做 AbortMultipartUpload。

### H6. 无 restart resume,orphan multipart 永存 (Review 3)

进程重启,offline_tasks 里的 in-flight R2 multipart 无人回收,R2 按 part 计费直到 lifecycle rule 清扫 (可能几十天)。

**必修**:
- Init 里扫 `status IN ('pending','running')` 的 offline_tasks
- v1 策略: 全部 AbortMultipartUpload + 标记 failed (用户手动重传)
- v2 策略: 从 parts_etags 续跑

### H7. VFS 丢失 = 数据不可恢复 (Review 3)

real_key 用 UUID,VFS SQLite 坏了/丢了,R2 里就是一堆 UUID 命名的 blob,无法反向映射回虚拟路径。

**必修**:
- 在 R2 对象 metadata 里塞 `X-Amz-Meta-Virtual-Path: /movies/x.iso`,作为反向索引兜底
- Complete 后追加一个 `_index.json` 到每个 R2 bucket (包含所有虚拟路径映射),定期更新
- 或至少在文档警示"必须定期备份 SQLite"

### H8. List placeholder 会被点击导致 500 (Review 1, Review 3)

placeholder 条目 `Mask: Temp`,用户点击 → driver.Link 找不到 real_key → 崩。

**必修**:
- placeholder 用 `model.Object{Mask: model.Temp}` 让前端识别为不可操作
- Link/Rename/Move/Remove 里对 in-flight 条目拒绝操作,返回明确错误
- 让 List() 用真实 name (`x.iso`) + Mask,不用 `x.iso (uploading 43%)` 伪造 name (避免破坏 join)

### H9. Bucket 从配置移除留下 dangling 引用 (Review 2, Review 3)

用户改 BucketsJSON 删掉一个 bucket,VFS 里 real_bucket 指向不存在的 bucket → Link/Remove 全崩。

**必修**:
- 配置校验: Init 里对比 VFS 里所有 distinct real_bucket 和当前配置,如果有 dangling refs 拒绝启动 (或提供 --force-drop-dangling flag)

### H10. presigned URL 泄露到 Worker 日志 (Review 3, Review 4)

Worker 的错误路径可能 dump request body (含 target_url + 签名)。任何有 tail-log 权限的人都能重放 PUT。

**必修**:
- Worker `/transfer-part` 里对 target_url 做 log redaction (只 log host + partN,不 log query string)
- 缩短 UploadPart presigned URL TTL 到 15 分钟

### H11. CFWorkerAuth 无 rotation,泄露 = SSRF-as-a-service (Review 4)

X-Auth 静态字符串,泄露后任何人能用你的 Worker 做 SSRF。

**必修**:
- 支持双 token (current + previous),支持在线 rotation
- 或 auth 从静态 token 改成时间戳 HMAC (driver + worker 共享 secret)
- v1 至少加个"轮换步骤"文档

### H12. custom_host 无验证 = phishing (Review 4)

用户如果配错/被诱导配置 custom_host 为攻击者域名,所有下载 302 到攻击者站。

**必修**:
- Init 里对 custom_host 做 HEAD 探测,验证它确实映射到 R2 bucket
- 或至少 admin UI 明确警告"custom_host 必须是 CF 侧 R2 public custom domain"

---

## Medium 级问题

### M1. `errs.NotImplement` vs 阻塞返回 — PutURL 语义选择

综合 C1,选定策略。三选一:

- **策略 A**: PutURL **同步阻塞**直到 R2 完成 → 简单,但用户请求会挂几分钟
- **策略 B**: PutURL 立即返回 `errs.NotImplement`,让 SimpleHttp fallback 到 tache DownloadTask → 用现有 offline_download 框架,前端能看进度,但 driver 侧还是要做实际搬运 → 需要 driver 自己实现 Tool 接口 + 注册 (回到最初 Tool 方案)
- **策略 C**: PutURL 同步阻塞,但在开始就写一个 VFS 条目 (Mask: Temp),List 能看到,失败时清理 → 折中

推荐 **策略 C**。首版接受 PutURL 挂几分钟 (HTTP long request)。前端 loading spinner + 用户可以关掉页面,driver 继续跑。

### M2. `model.FileStreamer` 不可 seek,s3manager 内存膨胀 (Review 2)

s3manager 需要 seekable 做 retry,不可 seek 时 buffer 每 part 全内存。200GB 文件 × PartSize=64MB × concurrency=5 = 320MB 峰值内存。

**必修**: 用 aws s3manager 显式设 PartSize + Concurrency,并在 Config 里限制 (`Put` 时先测 stream 大小,大文件降 concurrency)。

### M3. Presign GET 3600s 中途失效 (Review 2, Review 4)

慢速下载超过 1h → 403 on Range retry。

**必修**: Presign GET 有效期改成 24h;或 Link() 返回时用最长有效期。**不要**给 UploadPart 用同样长的 TTL (只需 15min)。

### M4. VFSDBPath 模板未定义, storage_id 不稳定 (Review 3, Review 4)

`{storage_id}.db` 依赖 storage.ID (DB autoincrement)。用户删+重加 mount → ID 变 → 老 SQLite 孤儿。也有 path traversal 风险 (如果 storage.ID 从 API 来)。

**必修**:
- v1: 用一个稳定 UUID (mount 创建时生成),存在 Addition 里
- Init 里对最终路径做 `filepath.Clean` + prefix 检查 (必须在 openlist data dir 内)

### M5. Cross-endpoint Copy fallback 消耗 VPS (Review 3)

NotSupport → OpenList 走 Stream Copy → Link (302 R2) → OpenList 服务器跟随 302 下载 → 再 Put → 消耗 VPS 双向流量。

**必修**: 文档警示,推荐用户避免跨 endpoint 复制。

### M6. ETag 引号规范化 (Review 2)

R2 返回 `"abcd..."` 带引号。SDK 一般会处理但 Worker → JSON → driver 这条链可能双引号或去引号。

**必修**: Worker 收到 ETag header 后 trim quote,driver 侧 Complete 时不加引号 (SDK 自会加)。加显式单元测试。

### M7. R2 checksum 兼容其他 (Review 2 + operational)

除 H1 外,某些 R2 endpoint 变种 (Cloudflare Enterprise 私有 R2) 可能 checksum 行为不同。文档加 troubleshooting 段落。

### M8. Balancer hash 策略破坏 rename (Review 3)

hash(virtual_path) % N 用于选桶,但 rename 后 path 变了 real_bucket 不变。策略只是**放置提示**,不是不变量。

**修**: 文档明确说明 hash 只是初始分配。

### M9. AbortMultipartUpload lifecycle rule 缺失 (Review 3, Review 4)

R2 bucket 默认不自动清理未完成 multipart。orphan 会持续计费。

**必修**: 文档要求用户为 R2 bucket 配置 `AbortIncompleteMultipartUpload` lifecycle (24h 或 7d 自动清理)。

### M10. Meta.Config() 未定义关键字段 (Review 1)

Config 有 NoCache, LocalSort, OnlyIndices, DefaultRoot, NoOverwriteUpload 等,文档只给 Name。

**必修**: 明确列出:
```go
Config{
    Name:                 "s3-balance",
    LocalSort:            true,   // VFS 用 SQL ORDER
    OnlyLinkMFile:        false,
    OnlyProxy:            false,
    NoCache:              false,  // 用 OpenList dir cache
    NoUpload:             false,
    NeedMs:               false,
    DefaultRoot:          "/",
    CheckStatus:          true,   // 让 Init 失败时 storage 显示 disconnect
    NoOverwriteUpload:    false,
    ProxyRangeOption:     false,
}
```

### M11. Meta.GetStorage/SetStorage 遗漏 (Review 1)

Driver 结构必须 embed `model.Storage`,文档没写。

**必修**: driver.go 骨架里显式写:
```go
type Driver struct {
    model.Storage
    Addition
    // ...
}
```

### M12. 多 OpenList instance / LB 问题 (Review 3)

SQLite 不适合多写。文档明确 v1 只支持单实例。

**必修**: 文档 §17 加限制。

### M13. R2 public bucket + custom_host 前提 (Review 2)

custom_host 只有配置成 R2 public bucket + CF custom domain 才工作。文档没交代前提。

**必修**: 文档加 "how to setup custom_host" 章节。

### M14. Duplicate bucket entries 无 dedup (Review 4)

BucketsJSON 里同 endpoint+bucket 出现两次,balancer 认为是两个 → 覆盖风险。

**必修**: Init 里检测重复,拒绝启动。

---

## Low 级问题

### L1. UUID entropy 未指定 (Review 4)

`generateRealKey` 用 UUID → 必须 crypto/rand 后端 (`github.com/google/uuid` v1.4+ 默认 v4 用 crypto/rand,但要显式验证)。

**修**: 用 `github.com/google/uuid` 的 `NewRandom()`,不用 `New()`。

### L2. Rename/Move placeholder join 破坏 (Review 3)

用 name 作 join key,rename 后就断了。

**修**: offline_tasks 表加 task_id (UUID),VFS placeholder 通过 task_id 关联,不通过 name。

### L3. Backup story 缺失 (Review 3)

SQLite 位置和备份策略未提。

**修**: 文档 §Backup 章节。

### L4. AK/SK 明文 (Review 2, Review 4)

跟现有 S3 driver 一致,但多桶放大风险。

**修**: 文档 §17 明确写"credential 与其他 S3 driver 同等级",future work 提 KMS/env 引用。

### L5. Multipart part_size 硬编码 (Review 2)

99614720 (~95MB) 对 Free Worker 100MB body 是合理的,但没考虑 Paid Worker 500MB。

**修**: part_size 从 Addition 读,不是常量。

---

## 建议的文档结构调整

原文档 20 节,新版需要新增/大改:

- 新增 §5.5 "Config() 值定义" (M10)
- 新增 §5.7 "resume-on-startup 逻辑" (H6)
- 大改 §6 "PutURL 语义" — 明确策略 C (M1, C1)
- 大改 §8 "PutURL 内部流程" — 加 sanitize + validate + Content-Length + checksum settings (C2, H1, H2, H10)
- 大改 §9 "Worker 契约" — Content-Length fallback + host 白名单 + log redaction (C2, H2, H10)
- 新增 §12.5 "credential 校验 (Init HeadBucket)" (H4)
- 大改 §14 "List placeholder" — 用 Mask:Temp 不改 name (H8, L2)
- 新增 §15.5 "R2 lifecycle rule 要求" (M9)
- 新增 §16.5 "custom_host 前提" (M13, H12)
- 大改 §17 "Risks" — 全部 finding 分类落入 (多 findings)
- 新增 §21 "Testing checklist for critical fixes"

---

## Verifier pass

综合完毕。下一步:
1. 应用所有 fix 到主 spec 文档
2. 跑一次 verifier agent,让它确认所有 finding 都被 spec 覆盖到,没有引入新矛盾

预计新版 spec ~600 行,是当前的 ~2 倍长度。合理,因为把所有 corner case 都写死了。
