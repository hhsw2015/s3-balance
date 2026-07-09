# OpenList `s3-balance` Driver 规格 (v2)

分支源: s3-balance `feat/offline-download-poc` @ `060a354`
目标项目: `/Users/wowdd1/Dev/OpenList`
目标位置: `drivers/s3_balance/`

**v2 变更**: 经四轮独立 adversarial review, 修复 33 个 findings (2 critical / 12 high / 14 medium / 5 low)。详见 `openlist-migration-review.md`。

## 1. 定位

**一个全新独立的 OpenList driver, 名为 `s3-balance`**。它把多个 R2 桶聚合成一个逻辑挂载点。写入路径可选走 CF Worker 白嫖 CF 流量。

**与独立 s3-balance Go 服务的关系**: 无代码依赖。同一套思想的独立重实现,面向 OpenList 场景。

## 2. 用户视角

**挂载**: OpenList admin → Add Storage → driver = `s3-balance` → 填多个 R2 桶配置 + 可选 CF Worker → Init 时会对每个 bucket 做 HeadBucket 校验 credential,失败即 storage status=disconnect。

**看**: 目录树,文件大小。目录/文件都由 driver-local SQLite (VFS) 决定,不 list R2。

**上传本地文件 (Put)**: balancer 挑桶,AWS SDK 分片上传 (消耗 VPS 上行,标准 driver 行为)。

**添加离线下载 (URL)**: 走 driver.PutURL,**同步阻塞**直到 R2 里对象就绪 (见 §6.2)。VPS 零字节 (只发命令给 Worker)。

**下载**: driver.Link 返回 R2 presigned URL (24h 有效) 或 custom_host,浏览器 302 直连 R2 (VPS 零字节)。

**移动/重命名**: 只改 SQLite,不动 R2 对象 (零成本)。

## 3. 核心不变式

1. **单一挂载点**: 用户看到一个 storage,内部多桶
2. **虚拟命名空间**: virtual path 与 R2 real_key 无关,只由 SQLite 决定。R2 里 real_key 是 UUID
3. **反向索引兜底**: 每个 R2 对象的 metadata 里存 `X-Amz-Meta-Vpath` = base64 encoded virtual path (SQLite 丢时可恢复)
4. **字节走向**:
   - 上传本地 Put: browser → VPS → R2
   - 离线上传 PutURL: source → CF Worker → R2 (**零 VPS**)
   - 下载 Link: R2 → browser (**零 VPS**)
5. **credential 只在 driver 侧**: Worker 用 driver 生成的短期 presigned URL,自身无长期凭证
6. **依赖方向**: OpenList driver → CF Worker (单向),Worker 从不 callback
7. **单实例部署**: 不支持多 OpenList instance / LB (SQLite 单写)

## 4. Addition (用户配置)

```go
type Addition struct {
    driver.RootPath   // 挂载根

    // 多桶配置 (JSON 数组字符串)
    BucketsJSON string `json:"buckets" type:"text" required:"true"
        help:"JSON array; each: {name,endpoint,region,ak,sk,bucket,max_size,weight,path_style,custom_host}"`

    // Balancer
    BalancerStrategy string `json:"balancer_strategy" type:"select"
        options:"round-robin,hash" default:"round-robin"`

    // Offline download (CF Worker)
    EnableOffline       bool   `json:"enable_offline"`
    CFWorkerURL         string `json:"cf_worker_url" help:"e.g. https://xxx.workers.dev"`
    CFWorkerAuth        string `json:"cf_worker_auth"`
    CFWorkerAuthPrev    string `json:"cf_worker_auth_prev" help:"previous token during rotation window (optional)"`
    CFWorkerPartSize    int64  `json:"cf_worker_part_size" default:"99614720" help:"Free Worker: <= 99614720 (~95MB)"`
    CFWorkerConcurrency int    `json:"cf_worker_concurrency" default:"2"`

    // Source URL allowlist (for SSRF防御)
    // 空 = 只拒绝 loopback/private/link-local; 非空 = 只允许这些 host 前缀
    SourceHostAllowlist string `json:"source_host_allowlist" type:"text"
        help:"newline-separated allowed host suffixes; empty means: allow public IPs only"`

    // Presigned URL 有效期
    PresignGetExpireSec int `json:"presign_get_expire_sec" default:"86400" help:"24h; long enough for slow downloads"`
    PresignPutExpireSec int `json:"presign_put_expire_sec" default:"900"   help:"15min; UploadPart internal use"`

    // VFS DB 稳定标识 (mount 创建时生成 UUID; 用户不改)
    VFSDBSlug string `json:"vfs_db_slug" help:"leave empty on first mount; auto-generated"`
}
```

`BucketConfig` (JSON 数组元素):
```json
{
  "name":         "r2-01",
  "endpoint":     "https://<acc>.r2.cloudflarestorage.com",
  "region":       "auto",
  "ak":           "...",
  "sk":           "...",
  "bucket":       "pool-01",
  "max_size":     "50GB",
  "weight":       10,
  "path_style":   true,
  "custom_host":  "cdn.example.com"
}
```

**Init 时对每个 bucket 校验**:
- 语法: bucket name 非空 + endpoint https
- 唯一性: (endpoint, bucket) 组合不能重复出现
- 连通性: `HeadBucket` 成功
- 权限: 探测 CreateMultipartUpload + AbortMultipartUpload 可用
- (可选) custom_host 校验: 对 `https://<custom_host>/<random-key>` 做 HEAD,期望 404 (证明它是绑到 R2 的域名)

任一失败 → Init 返回 error, storage status=disconnect。

## 5. VFS Schema

### 5.1 SQLite 位置

- 相对路径 `data/s3_balance/<vfs_db_slug>.db` (相对 OpenList data-dir)
- `vfs_db_slug` 首次 Init 时自动生成 UUIDv4,写回 Addition (存 OpenList 主库),后续挂载稳定
- `filepath.Clean` + 强制前缀 `data/s3_balance/` 防 traversal

### 5.2 Schema

```sql
CREATE TABLE virtual_entries (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    parent_path  TEXT    NOT NULL,   -- 规范化 (/movies, 无尾斜杠, 根 = "/")
    name         TEXT    NOT NULL,
    is_dir       INTEGER NOT NULL,
    size         INTEGER NOT NULL DEFAULT 0,
    real_bucket  TEXT,               -- 目录/placeholder 为 NULL
    real_key     TEXT,               -- 目录/placeholder 为 NULL
    etag         TEXT,               -- 无引号
    mime         TEXT,
    mask         INTEGER NOT NULL DEFAULT 0,  -- model.Mask (Temp=1 表示 in-flight)
    task_id      TEXT,               -- 关联 offline_tasks; placeholder 用它 join
    modified_at  INTEGER NOT NULL,   -- unix ms
    created_at   INTEGER NOT NULL,
    UNIQUE(parent_path, name)
);
CREATE INDEX idx_parent ON virtual_entries(parent_path);
CREATE INDEX idx_real   ON virtual_entries(real_bucket, real_key);
CREATE INDEX idx_task   ON virtual_entries(task_id);

CREATE TABLE offline_tasks (
    id           TEXT PRIMARY KEY,
    source_url   TEXT NOT NULL,
    parent_path  TEXT NOT NULL,
    name         TEXT NOT NULL,
    real_bucket  TEXT NOT NULL,
    real_key     TEXT NOT NULL,
    upload_id    TEXT NOT NULL,
    size         INTEGER,
    part_size    INTEGER NOT NULL,
    parts_total  INTEGER NOT NULL,
    parts_done   INTEGER NOT NULL DEFAULT 0,
    parts_etags  TEXT,               -- JSON [{n, etag}, ...]
    status       TEXT NOT NULL,      -- pending|running|done|failed|aborted
    error        TEXT,
    created_at   INTEGER NOT NULL,
    updated_at   INTEGER NOT NULL
);
CREATE INDEX idx_status ON offline_tasks(status);
```

### 5.3 反向索引兜底 (M/H7)

每次 CreateMultipartUpload 时,把 `X-Amz-Meta-Vpath` = base64url(virtual_path) 写进 metadata。**SQLite 完全丢失时,可从 R2 遍历 metadata 恢复所有虚拟路径映射**。文档提供 recovery 脚本。

### 5.4 孤儿数据处理

- **VFS 有条目 R2 无对象**: Link 时 HeadObject 404 → warning log + VFS 删条目
- **R2 有对象 VFS 无条目**: driver 不看,由 recovery 脚本恢复
- **配置移除 bucket 但 VFS 有 ref**: Init 拒绝启动,除非 admin 在 Addition 里显式设置 `AllowDanglingBuckets: true`

## 6. Config + Driver 接口实现

### 6.1 Meta.Config()

```go
Config{
    Name:              "s3-balance",
    LocalSort:         true,     // VFS 用 SQL ORDER
    OnlyLinkMFile:     false,
    OnlyProxy:         false,
    NoCache:           false,    // 依赖 OpenList dir cache 减少 SQLite 查
    NoUpload:          false,
    NeedMs:            false,
    DefaultRoot:       "/",
    CheckStatus:       true,     // 让 Init 失败时 storage 显示 disconnect
    NoOverwriteUpload: false,
    ProxyRangeOption:  false,
}
```

Driver 结构:
```go
type Driver struct {
    model.Storage   // embed, 提供 GetStorage/SetStorage
    Addition        // embed, 提供 GetAddition
    // 私有字段
    vfs      *VFS
    pool     *BucketPool
    balancer Balancer
    workerC  *WorkerClient
    // 在飞的 offline task 索引 (task_id → cancelFunc, 供 driver.Other 取消)
    inflight sync.Map           // map[string]context.CancelFunc
    // 全局 ctx: Init 时创建, Drop 时 cancel 触发所有 goroutine 退出
    ctx    context.Context
    cancel context.CancelFunc
    // 用来等 Drop 时所有 goroutine 退出
    wg sync.WaitGroup
}
```

**关于 tache**: v1 **不使用 tache**。理由: PutURL 是同步阻塞 (§6.2),内部并发用 `errgroup.WithContext` + `SetLimit`,没有独立 task 生命周期需要 tache 管。tache 引入会:
1. 与"同步阻塞"矛盾 (tache.Add 是异步入队)
2. 无 persistence 时对 restart 无价值 (offline_tasks 表已经是持久化真相)
3. driver.Other 直接查 offline_tasks 表即可暴露 list/cancel

**未来 v2 若改成异步 PutURL** (返回 task id, 走 SimpleHttp fallback), 才引入 tache。当前不引入。

### 6.2 PutURL 语义 (Critical, C1)

**决策: 策略 C (同步阻塞 + 立即写 placeholder)**

```go
func (d *Driver) PutURL(ctx, dstDir, name, sourceURL string) error {
    // 1. 安全校验 (SSRF 防御, §8.1)
    if err := d.validateSourceURL(sourceURL); err != nil {
        return err
    }
    // 2. balancer 挑桶
    bucket := d.balancer.Select(name)
    realKey := d.newRealKey()   // UUIDv4 from crypto/rand
    virtualPath := path.Join(dstDir.GetPath(), name)
    
    // 3. HEAD probe via Worker
    size, err := d.workerC.Probe(sourceURL)
    if err != nil {
        return err  // 失败即失败, PutURL 不返回 nil
    }
    
    // 4. CreateMultipartUpload with metadata
    uploadID, err := d.s3Create(bucket, realKey, size, virtualPath)
    if err != nil {
        return err
    }
    
    // 5. 写 offline_tasks + placeholder VFS entry
    taskID := uuid.NewString()
    d.vfs.PutOfflineTask(taskID, ...)  // status=running
    d.vfs.PutPlaceholder(virtualPath, size, taskID)  // mask=Temp
    
    // 6. 同步执行 (阻塞直到完成或失败)
    err = d.runOfflineTaskBlocking(ctx, taskID)
    if err != nil {
        d.vfs.MarkTaskFailed(taskID, err.Error())
        d.vfs.RemovePlaceholder(taskID)
        _ = d.s3Abort(bucket, realKey, uploadID)  // best-effort
        return err
    }
    
    // 7. 完成: 把 placeholder 升级成真实条目
    d.vfs.CompleteOfflineTask(taskID)
    // 此时 VFS 里 mask=0, real_bucket/real_key 已填, size 已确认, etag 已存
    return nil
}
```

**关键**: 阻塞返回 nil = R2 里对象真的就绪了。这样 `op.PutURL` 插入的 Temp obj 会在下次 List 时被真实 obj 替代。用户体验: 提交后 HTTP 请求挂几十秒~几分钟,然后返回成功,页面 refresh 看到文件。

**长请求处理**: OpenList HTTP 层默认无严格 timeout,几分钟可以接受。前端 UI 显示 loading。**若用户关掉页面**,driver goroutine 因 `ctx.Done` 被取消 → Abort multipart + mark failed。

### 6.3 List 语义

```go
func (d *Driver) List(ctx, dir, args) ([]model.Obj, error) {
    entries := d.vfs.List(dir.GetPath())
    objs := make([]model.Obj, 0, len(entries))
    for _, e := range entries {
        obj := &model.Object{
            Name:     e.Name,                    // 真实名字, 不加 "(uploading)"
            Size:     e.Size,
            Modified: time.UnixMilli(e.Modified),
            IsFolder: e.IsDir,
        }
        if e.Mask != 0 {
            objs = append(objs, &model.ObjWrapMask{Obj: obj, Mask: e.Mask})
        } else {
            objs = append(objs, obj)
        }
    }
    return objs, nil
}
```

**前端识别 Mask:Temp**: OpenList 已有惯例,前端会显示为半透明或加 "processing" 标记。用户不会误点。

### 6.4 Link 对 placeholder 明确拒绝

```go
func (d *Driver) Link(ctx, file, args) (*model.Link, error) {
    entry := d.vfs.Get(file.GetPath())
    if entry.Mask&model.Temp != 0 {
        return nil, errors.New("file is still being transferred")
    }
    bucket := d.pool.Get(entry.RealBucket)
    if bucket == nil {
        return nil, fmt.Errorf("bucket %q not in pool (dangling)", entry.RealBucket)
    }
    var url string
    if bucket.CustomHost != "" {
        url = fmt.Sprintf("https://%s/%s", bucket.CustomHost, entry.RealKey)
    } else {
        url = bucket.PresignGet(entry.RealKey, d.PresignGetExpireSec)
    }
    return &model.Link{URL: url}, nil
}
```

### 6.5 Move / Rename / Remove 对 placeholder 拒绝

```go
func (d *Driver) Rename(ctx, srcObj, newName) error {
    e := d.vfs.Get(srcObj.GetPath())
    if e.Mask&model.Temp != 0 {
        return errors.New("cannot rename in-flight upload")
    }
    return d.vfs.UpdateName(srcObj.GetPath(), newName)
}
```

`Move` 同理。`Remove` 对 placeholder 特殊处理: 先 Abort multipart + 清 offline_tasks + 清 placeholder。

### 6.6 Meta.Init(ctx)

顺序:
1. 解析 `BucketsJSON`; 校验唯一性、字段完整、URL 合法
2. 若 `VFSDBSlug` 为空, 生成 UUIDv4 并回写 Addition (会触发 OpenList 保存)
3. 打开 SQLite (path = `data/s3_balance/<slug>.db`, 校验前缀防 traversal)
4. 建表 (idempotent)
5. 建 BucketPool: 每个 bucket 一个 aws-sdk-go-v2 S3 client
   - **必须**: `RequestChecksumCalculation = WhenRequired`, `ResponseChecksumValidation = WhenRequired` (H1)
   - `UsePathStyle = true` (R2 要求)
6. 对每个 bucket: HeadBucket (H4). 任一失败: 返回 error → storage disconnect
7. VFS 一致性检查: `SELECT DISTINCT real_bucket FROM virtual_entries` 里的每个都必须在 BucketPool 里. 否则拒绝启动 (除非 `AllowDanglingBuckets`)
8. **Resume-on-startup** (H6):
   ```sql
   SELECT id FROM offline_tasks WHERE status IN ('pending','running')
   ```
   对每个: 调用 R2.AbortMultipartUpload (best-effort, 失败继续) + 更新 status='aborted' + 删除 placeholder VFS entry. v1 不做真续跑
9. 建 driver ctx (可 cancel) — 用于 Drop 时统一取消所有 goroutine

### 6.7 Meta.Drop(ctx)

顺序:
1. `d.cancel()` — 通知所有 driver goroutine 退出
2. `d.wg.Wait()` — 等所有 in-flight PutURL goroutine 退出 (它们会因 ctx.Done 提前 Abort R2)
3. 对**残留** status=running 的 offline_tasks 兜底: AbortMultipartUpload + status='aborted' (正常情况 goroutine 已经处理过)
4. 关 SQLite

## 7. Copy 语义

- **同 bucket 内**: 走 R2 CopyObject. **超过 5GB (H3)**: 走 UploadPartCopy 分片
- **跨 bucket (同 endpoint)**: R2 CopyObject 支持跨 bucket (在源 bucket client 上调, 目标 bucket 作为 target). 同样 >5GB 走分片
- **跨 endpoint**: `return errs.NotSupport`. OpenList fallback = Stream get-then-put (走 VPS). **文档警示**

## 8. PutURL 详细流程 + 安全

### 8.1 Source URL 校验 (Critical, C2 SSRF)

```go
func (d *Driver) validateSourceURL(raw string) error {
    u, err := url.Parse(raw)
    if err != nil { return err }
    if u.Scheme != "https" && u.Scheme != "http" {
        return errors.New("only http(s) allowed")
    }
    host := u.Hostname()
    // DNS resolve
    ips, err := net.LookupIP(host)
    if err != nil { return err }
    for _, ip := range ips {
        if ip.IsLoopback() || ip.IsPrivate() || ip.IsLinkLocalUnicast() ||
           ip.IsMulticast() || ip.IsUnspecified() {
            return fmt.Errorf("forbidden ip %s", ip)
        }
        // 显式黑名单
        if ip.Equal(net.ParseIP("169.254.169.254")) {
            return errors.New("metadata service blocked")
        }
    }
    // Allowlist (if configured)
    if len(d.SourceHostAllowlist) > 0 {
        allowed := false
        for _, suf := range strings.Split(d.SourceHostAllowlist, "\n") {
            if strings.HasSuffix(host, strings.TrimSpace(suf)) {
                allowed = true; break
            }
        }
        if !allowed { return fmt.Errorf("host %q not in allowlist", host) }
    }
    return nil
}
```

**注意**: 校验后到 Worker 实际 fetch 之间存在 TOCTOU (DNS rebinding). Worker 侧再做一次同类校验 (§9).

### 8.2 主流程

```
1. validateSourceURL(url) — SSRF 防御
2. bucket = balancer.Select(name)
3. real_key = crypto/rand UUIDv4
4. workerC.Probe(url) → size (通过 Worker HEAD, 若失败 fallback GET Range 0-0)
5. s3.CreateMultipartUpload(bucket, real_key, metadata: {vpath: base64(virtual)})
   → upload_id
6. INSERT offline_tasks (status=running)
7. INSERT virtual_entries (mask=Temp, task_id, parent, name, size)
8. for partN in 1..N:
    range = [partN-1)*part_size, min(size, partN*part_size) - 1]
    presigned = s3.PresignUploadPart(bucket, real_key, upload_id, partN, ttl=PresignPutExpireSec)
    // presigned 里含短期签名 (15min)
    resp = workerC.TransferPart(source_url, source_range=range,
                                 target_url=presigned,
                                 target_content_length = range_size)
    // 
    etag = trimQuotes(resp.etag)
    UPDATE offline_tasks SET parts_done, parts_etags
9. s3.CompleteMultipartUpload(bucket, real_key, upload_id, parts_etags)
10. UPDATE virtual_entries SET real_bucket, real_key, etag, mask=0 WHERE task_id=?
11. UPDATE offline_tasks SET status=done
```

**并发**: partN 用 goroutine pool (CFWorkerConcurrency=2 by default). errgroup + SetLimit 控并发。

**取消**: PutURL 的 ctx 一旦 Done → cancel 所有 goroutine → Abort multipart。

### 8.3 Presigned URL 生成

```go
presignClient := s3.NewPresignClient(bucket.client)
req, err := presignClient.PresignUploadPart(ctx, &s3.UploadPartInput{
    Bucket:        &bucket.name,
    Key:           &realKey,
    UploadId:      &uploadID,
    PartNumber:    aws.Int32(int32(partN)),
    // ContentLength 被 aws-sdk-go-v2 加入 signed headers
    // Worker PUT 必须以完全一致的 Content-Length 发送, 否则 SigV4 mismatch
    ContentLength: aws.Int64(rangeSize),
}, func(o *s3.PresignOptions) {
    o.Expires = time.Duration(d.PresignPutExpireSec) * time.Second
})
return req.URL, rangeSize   // 返回 URL 和 length, 一起传给 Worker
```

**关键**:
- Client 必须开 `RequestChecksumCalculation = WhenRequired` (H1)
- Bucket client 用 `UsePathStyle=true` (R2 兼容)
- **签名 length 一致性**: driver 传给 Worker 的 `target_content_length` 必须与 `PresignUploadPart` 用的 `ContentLength` 相同,精确到字节。**单元测试要 assert 这一点** (§15.T-lengthparity)

## 9. Worker 契约 (`/transfer-part`)

### 9.1 Request/Response

```
POST /transfer-part
Header: X-Auth: <shared token>  (支持 current 或 previous)
Body: {
  source_url: string,
  source_headers?: {[k]: string},
  source_range: {start: number, end: number},
  target_url: string,           // R2 presigned UploadPart URL
  target_content_length: number // rangeEnd - rangeStart + 1
}

Response 200:
{
  ok: true,
  etag: string,                 // 无引号
  status: number                // 目标 PUT 的 HTTP status
}

Response 4xx/5xx:
{
  error: string,
  status: number
}
```

### 9.2 Worker 实现要点

```js
async function handleTransferPart(request, env) {
  // Auth: 双 token 支持 (rotation)
  const tok = request.headers.get("X-Auth");
  if (tok !== env.AUTH_CURRENT && tok !== env.AUTH_PREVIOUS) {
    return json({error: "unauthorized"}, 401);
  }

  const { source_url, source_headers, source_range, target_url,
          target_content_length } = await request.json();

  // Source URL 二次校验 (防 driver 侧 TOCTOU / DNS rebind)
  if (!isSafeURL(source_url)) {
    return json({error: "source blocked"}, 400);
  }

  // fetch source
  const srcHdr = { ...(source_headers || {}) };
  if (source_range) {
    srcHdr.Range = `bytes=${source_range.start}-${source_range.end}`;
  }
  const srcRes = await fetch(source_url, { headers: srcHdr });
  if (!srcRes.ok && srcRes.status !== 206) {
    // 不 log target_url (可能含 signature)
    return json({error: "source fetch failed", status: srcRes.status}, 502);
  }

  // PUT to target presigned URL
  // 关键: Content-Length 从入参拿, 不从 srcRes.headers 拿 (H2)
  const putRes = await fetch(target_url, {
    method: "PUT",
    body: srcRes.body,
    headers: { "Content-Length": String(target_content_length) },
  });
  if (!putRes.ok) {
    // 不 log body (可能被 R2 回显 target key)
    return json({error: "target PUT failed", status: putRes.status}, 502);
  }

  const etag = (putRes.headers.get("ETag") || "").replace(/^"|"$/g, "");
  return json({ ok: true, status: putRes.status, etag });
}

function isSafeURL(u) {
  try {
    const url = new URL(u);
    if (!["http:","https:"].includes(url.protocol)) return false;
    // Worker 无法本地 DNS 解析预校验; 依赖 CF fetch 拒绝 internal-only 地址.
    // 显式 host 黑名单:
    const badHosts = ["169.254.169.254","metadata.google.internal","localhost"];
    if (badHosts.includes(url.hostname)) return false;
    return true;
  } catch { return false; }
}
```

**Log 卫生 (H10)**:
- 不 log `target_url` (含 signature)
- 不 log request body 全文
- 错误只 log host + status

**Auth rotation (H11)**:
- Worker 存 `AUTH_CURRENT` + `AUTH_PREVIOUS` (workers secret)
- 双 token 都接受
- 用户 rotation: 生成新 token → 部署 `AUTH_PREVIOUS=旧, AUTH_CURRENT=新` → driver 切到新 → 一段时间后清空 `AUTH_PREVIOUS`

## 10. Balancer

### 10.1 策略 (首版)

- **round-robin**: 全局计数器 % N (bucket 数量, 未禁用的)
- **hash**: `hash(virtual_path) % N`. 只是**初始分配提示** (M8), rename 后 real_bucket 不变

**首版不做**: least-space (需要 R2 usage tracking, 复杂), weighted (类似)

### 10.2 Bucket 去重 (M14)

Init 里对 `(endpoint, bucket)` 元组去重. 重复视为 config error, 拒绝启动.

## 11. Tache Manager 集成

**v1 不使用 tache**。理由见 §6.1。

**并发模型**: 每个 PutURL 调用是一个独立 goroutine (被 OpenList HTTP handler 阻塞等着),内部用 `errgroup.WithContext(d.ctx, request.ctx merged)` + `SetLimit(CFWorkerConcurrency)` 跑分片。

**持久化**: offline_tasks 表就是持久化真相。restart resume 见 §6.6 step 8。

**前端可见性**: 首版无独立进度页面. 用户看进度的方式:
- 提交 PutURL 后 HTTP 请求挂起, 前端 loading spinner
- 关掉页面 → request ctx cancel → goroutine 收到 → Abort R2 + 清 VFS placeholder + status='aborted'
- 想看历史: `driver.List` 会显示 mask=Temp 的 in-flight 条目, 完成后自动变正常

**Cancel/List admin API**: 通过 OpenList `driver.Other` 接口暴露:
```go
func (d *Driver) Other(ctx, args model.OtherArgs) (any, error) {
    switch args.Method {
    case "offline.list":
        // 查 offline_tasks 表返回
    case "offline.cancel":
        // args.Data 是 task_id; 从 inflight sync.Map 取 cancelFunc 调用
    case "offline.retry":
        // 只支持 status=failed/aborted 的任务重新起 (等价新提交)
    }
}
```
`driver.Other` 签名: `Other(ctx context.Context, args model.OtherArgs) (any, error)`,`model.OtherArgs{Method string, Data interface{}}` (`internal/driver/driver.go:28-29`,已验证)。

## 12. 目录结构

```
drivers/s3_balance/
├── driver.go              # Driver + 接口实现 (List/Link/Put/PutURL/Remove/Move/Rename/Copy/Mkdir/Init/Drop/Other)
├── meta.go                # Addition + Config + init 注册
├── types.go               # BucketRef, VirtualEntry, OfflineTaskRow
├── bucket_pool.go         # 多 R2 SDK client (with WhenRequired checksum)
├── balancer.go            # round-robin + hash
├── vfs.go                 # SQLite CRUD (modernc.org/sqlite pure-Go)
├── offline.go             # PutURL 编排 + tache OfflineTask
├── worker_client.go       # 调 /transfer-part
├── s3_client.go           # multipart, presign, HeadBucket
├── security.go            # validateSourceURL (SSRF)
├── recovery.go            # (未来) reindex from R2 metadata
└── util.go
```

## 13. 首版范围 (v1)

**必做**:
- driver.go 全部接口
- Put 走 aws-sdk-go-v2 s3manager (with checksum WhenRequired)
- PutURL 走 CF Worker (同步阻塞, mask=Temp placeholder)
- Balancer: round-robin + hash
- VFS SQLite + reverse metadata index
- Init HeadBucket 校验
- Resume-on-startup: abort orphan multipart
- Drop: cancel + wait + abort
- Worker `/transfer-part` 增量 + SSRF 校验 + auth rotation
- Copy 同/跨 bucket 同 endpoint (含 UploadPartCopy for >5GB)
- SSRF 防御 (driver + Worker 两层)
- Log 卫生
- driver.Other 暴露 task 管理

**不做**:
- Least-space / weighted balancer
- 前端进度专用 UI (改主项目)
- 跨 endpoint Copy (Worker 中转)
- 健康检查后台 goroutine (Init 时一次即可)
- Direct browser upload (browser → R2 presigned, 绕 VPS)
- WebDAV 扩展语义
- 多用户 per-bucket 权限
- 自动 reindex from R2 metadata (recovery 脚本, v2)
- Resume 真续跑 (v1 只 abort; v2 从 parts_etags 续)

## 14. R2 前置要求 (给用户的部署文档)

### 14.1 R2 Lifecycle Rule (M9)

每个 R2 bucket 必须配置 `AbortIncompleteMultipartUpload` lifecycle rule:

```
Days after initiation: 1
```

原因: 防止 driver 崩溃后的 orphan multipart 无限计费。1 天足够,driver 正常 Init/Drop 也会 Abort。

### 14.2 R2 API Token 权限

至少:
- Object Read
- Object Write
- Multipart Upload (含 Create/Upload/Complete/Abort)

### 14.3 (可选) Custom Host (M13)

想让下载走自己的域名 (代替 R2 presigned URL):
1. 在 CF dashboard R2 → bucket → Settings → Public Access 启用
2. 在 CF DNS 加 CNAME `cdn.example.com` → `<bucket>.<acc>.r2.cloudflarestorage.com`
3. 在 driver Addition 里填 `custom_host: "cdn.example.com"`

**注意**: custom host 是**公开访问**,任何知道 real_key 的人可下。real_key 是 UUID 不可猜,但仍是 security-through-obscurity。**敏感数据请用 presigned URL 模式** (不填 custom_host)。

### 14.4 (可选) Backup

用户责任:
- 定期备份 `data/s3_balance/<slug>.db` (SQLite)
- v2 将提供 `driver.Other("backup_index_to_r2")` 一键把 index 上传到 R2 兜底

## 15. 端到端测试计划

**前置**:
- OpenList dev build
- 至少 2 个 R2 bucket
- CF Worker 部署新版 `worker.js` (含 `/transfer-part`, SSRF check, auth rotation)

**测试点** (每一项列出要 assert 的行为):

1. **挂载 + Init 校验**: 
   - 挂载 → HeadBucket 全过 → status=work
   - 故意填错 SK → Init 失败 → status=disconnect
2. **VFS 独立**: 不同 storage 有不同 SQLite 文件
3. **MakeDir**: 创建 /movies → List 看到目录
4. **Put small (10MB)**: 上传本地 → balancer 挑桶 → R2 里有对象 (含 Vpath metadata) → driver.List 看到
5. **Put large (200MB)**: 走 s3manager 分片 → 完成 ETag 带 `-N`
6. **PutURL small (100MB)**: 添加离线下载 → PutURL 挂起 ~10 秒 → 返回成功 → List 看到 (mask=0)
7. **PutURL large (1GB)**: 挂起 ~30-60 秒 → 完成
8. **PutURL 中途取消 (关页面)**: driver ctx cancel → R2 Abort → offline_tasks status=aborted → 无 orphan
9. **PutURL SSRF 拒绝**: source=`http://169.254.169.254/` → 立即返回 forbidden error
10. **PutURL 期间 List 看到 placeholder**: 传输中 → List `/movies/` 看到 name=x.iso, mask=Temp
11. **Link on placeholder**: 拒绝, 返回明确 error
12. **Link on completed**: 302 到 R2 presigned URL (24h TTL)
13. **Rename after complete**: SQLite 更新, R2 无操作, 新链接下载正常
14. **Rename on placeholder**: 拒绝
15. **Move**: 同上
16. **Remove**: R2 DeleteObject + VFS 删除
17. **Copy same-bucket small**: R2 CopyObject
18. **Copy same-bucket >5GB**: UploadPartCopy 分片
19. **Copy cross-endpoint**: NotSupport → fallback 走 VPS (确认能完成, 但会消耗流量)
20. **Multi-bucket distribution**: 20 个 upload, real_bucket 分布均匀 (round-robin)
21. **Bucket 从配置移除**: Init 拒绝启动, 报 dangling refs 数量
22. **Restart during PutURL**: kill OpenList 进程 → 重启 → Init 见到 status=running 的 task → 全部 Abort → VFS placeholder 清理
23. **Drop with in-flight**: 卸载 storage → cancel + wait + abort → SQLite 保留
24. **Reverse metadata recovery** (手动测): 删掉 SQLite → 从 R2 遍历 metadata → 重建 VFS (recovery 脚本)
25. **Auth rotation**: driver 切新 token → Worker 双 token 期间 → 老 token 30 分钟内仍可用 → 30 分钟后清空 previous
26. **Length parity**: 单元测试 assert `presignedUploadPart.ContentLength == workerRequest.target_content_length == rangeEnd - rangeStart + 1` 三者精确相等
27. **Step10 failure recovery**: 注入 SQLite 写失败 (mock disk full) → 观察 log 有 "step10_failed" → 重启 driver → Init resume 检测到并重跑 → 最终 VFS 有真实条目
28. **Nginx timeout**: 部署 nginx 前置 (默认 60s timeout) → 提交 1GB PutURL → 观察 502 → 调 `proxy_read_timeout=3600s` 后成功

## 16. 迁移工作量

| 项 | 复杂度 | 预估 |
|---|---|---|
| meta.go / Addition | 低 | 2 h |
| bucket_pool.go (含 checksum config) | 中 | 3 h |
| balancer.go | 低 | 1 h |
| vfs.go (SQLite CRUD + reverse index write) | 中 | 5 h |
| s3_client.go (multipart, presign, HeadBucket, metadata) | 中 | 4 h |
| worker_client.go | 低 | 1 h |
| security.go (SSRF check) | 低 | 2 h |
| offline.go (tache OfflineTask, cancel/abort) | 中高 | 6 h |
| driver.go (接口整合 + Init/Drop lifecycle) | 中高 | 6 h |
| Worker `/transfer-part` v2 (auth rotation + SSRF + logging) | 中 | 3 h |
| 编译 + 单元测试 (etag trim, path clean, SSRF matrix) | 中 | 4 h |
| 端到端测试 (§15 全部 25 项) | 中高 | 8 h |
| 文档 (README + 部署指南 + backup 说明) | 低 | 3 h |

**合计 ~48 小时纯写码, ~6-7 工作日**。比 v1 估算翻倍, 因为吸收了 review 后的所有 hardening。

## 16.5 边缘情况处理 (verifier 补充)

### 16.5.1 Step-10 SQLite 写失败,R2 已 Complete

**场景**: §8.2 step 9 CompleteMultipartUpload 成功,step 10 UPDATE virtual_entries 失败 (disk full / SQLite locked)。

**问题**: R2 里有真对象,VFS 还是 placeholder → 用户看不到 (mask=Temp),但已经计费。

**解决**: step 10 用**幂等重试**:
```go
for attempt := 0; attempt < 5; attempt++ {
    err := d.vfs.CompleteOfflineTask(taskID, etag)
    if err == nil { break }
    time.Sleep(1 << attempt * 100 * time.Millisecond)
}
if err != nil {
    // last resort: leave a marker file (rare edge case)
    log.Errorf("STEP10 FAILED for task %s, R2 object exists at %s/%s. Manual reconciliation needed.",
               taskID, bucket, realKey)
    // 不 abort R2 (数据已经在), 但 mark task_id in offline_tasks 有特殊 status='step10_failed'
    // Init resume 时会检测这个 status, 重新尝试 step 10
}
```

Init resume (§6.6 step 8) 扩展逻辑:
- `status='step10_failed'` → 重新执行 step 10 (从 offline_tasks 里读 real_bucket/real_key/etag, VFS 里升级 placeholder)

### 16.5.2 DNS 重绑 TOCTOU

**场景**: driver §8.1 校验时 hostname 解析到公网 IP,Worker 实际 fetch 时 DNS 已被重绑到内网 IP。

**缓解 (非完美)**:
- Worker `isSafeURL` 只能做静态 host blacklist (Worker 无 DNS API)
- 依赖 CF fetch 层对 IMDS-like 内网地址已有默认拒绝 (CF 声明,未 100% 保证)
- 用户可配置 `SourceHostAllowlist` 白名单,把攻击面收敛

**残留风险**: 允许公网 host 时,DNS rebind 仍可能穿透。用户需知这个限制。

### 16.5.3 长 PutURL 请求穿透反向代理

**问题**: nginx 默认 `proxy_read_timeout=60s`, PutURL 传 10GB 可能 5 分钟以上,会被反向代理 502。

**缓解**:
- 部署文档要求:
  - nginx: `proxy_read_timeout 3600s; proxy_send_timeout 3600s;`
  - Cloudflare 前置代理: 默认 100s,需要 Enterprise 升到长
  - haproxy: `timeout server 3600s`
- 或建议 OpenList 部署时**不加反向代理**,直接暴露 (仅内网)

**长期方案 (v2)**: PutURL 改成异步 → 立即返回 task_id → 前端轮询. 但需要改 OpenList 主项目 (让 SimpleHttp 认 driver 返回的 task 而不是 nil).

### 16.5.4 v1 backup 责任

- 用户责任: 每天 rsync `data/s3_balance/` 到别处
- 或利用 R2 metadata 反向索引,SQLite 丢时手动跑 recovery 脚本 (§14.4)
- v2 提供 `driver.Other("offline.backup_index")` 一键上传 SQLite 到 R2 特殊 key

## 17. 已识别风险 (最终版)

### 17.1 数据完整性

- **VFS 单点故障**: SQLite 丢失 → 依赖 R2 metadata 反向索引恢复 (§5.3, §15.24)
- **Reverse index 不完整**: 若某个对象因 bug 没写 metadata → 恢复不了。文档强调**Complete 前必须已写 metadata (由 CreateMultipartUpload 阶段写入)**
- **多 instance 部署会破坏**: SQLite 单写, 明确不支持

### 17.2 Credential 泄露

- BucketsJSON 明文存 storage row (AK/SK, CFWorkerAuth): **与其他 S3 driver 同等级**, 但多桶 = 更大 blast radius
- Presigned UploadPart URL 15min TTL: 短周期限制重放窗口
- Worker log 不记 target_url: 防 log-based 泄露
- Future: 用 env var / KMS 引用替代明文

### 17.3 SSRF

- 双层防御: driver 校验 (可预防, TOCTOU 存在) + Worker 校验 (兜底)
- 用户可配 `SourceHostAllowlist` 白名单进一步锁

### 17.4 长请求

- PutURL 挂几分钟: 依赖 OpenList HTTP 层无严格 timeout. 如果部署在 nginx 后需要调 proxy_read_timeout
- 用户关页面 = cancel: driver 通过 ctx 收到, Abort R2, 清理

### 17.5 Cross-endpoint Copy

- Fallback 到 stream copy, 消耗 VPS 流量. 文档警示. v2 用 Worker 中转解决

### 17.6 单 Worker 单点

- 一个 CFWorkerURL, 挂了 offline 就停. v1 接受. v2 可以支持多 URL round-robin

## 18. Verifier checklist (下一步执行)

跑一个 verifier agent 确认:
- [ ] 所有 review findings 都被本文档覆盖
- [ ] 没有引入新矛盾
- [ ] Config 值合理 (LocalSort=true 前提 VFS 排序稳定)
- [ ] Init/Drop 顺序无 race
- [ ] PutURL 同步语义与 op.PutURL 契约兼容 (返回 nil + err==nil = 对象已就绪)

## 19. 相关文件索引

**OpenList 已有代码 (只读参考)**:
- `internal/driver/driver.go` — 接口定义
- `internal/op/fs.go:715-770` — PutURL 契约 (关键)
- `internal/model/obj.go:22-34` — Obj/Mask 语义
- `drivers/s3/driver.go` — R2 交互参考
- `drivers/url_tree/driver.go:220` — PutURL 参考
- `internal/offline_download/tool/add.go:71` — SimpleHttp PutURL 触发
- `internal/task/base.go` — TaskExtension
- `server/handles/task.go:220-228` — task 路由 (硬编码, 不可扩展)

**独立 s3-balance 项目 (思想参考, 不 import)**:
- `internal/bucket/manager.go`
- `internal/balancer/*`
- `internal/offline/executor.go` — CF Worker 编排 PoC
- `internal/offline/types.go`

**新增/修改**:
- `drivers/s3_balance/` (全新)
- `drivers/all.go` (加 1 行 blank import)
- CF Worker `worker.js` (加 /transfer-part + SSRF check + auth rotation)

**关联文档**:
- `docs/openlist-migration-review.md` — 完整 review findings

## 20. Troubleshooting 附录

**症状 → 可能原因 → 检查**

| 症状 | 原因 | 检查 |
|---|---|---|
| Init 时 storage=disconnect | HeadBucket 失败 (AK/SK/endpoint) | 用 aws-cli 直接对 R2 做 `s3api head-bucket` 复现 |
| PutURL 返回 `invalid part` | ETag 引号未 trim / partN 错序 | Worker 返回 etag 是否已 trim quote; parts_etags 是否按 partN 有序 |
| PutURL 返回 `SignatureDoesNotMatch` | Content-Length 与 presign 时 ContentLength 不一致 | 断言 §16.5 length parity |
| PutURL 返回 `MalformedPOSTRequest` 或 checksum-related | aws-sdk-go-v2 checksum trailer 被 R2 拒 | 确认 `RequestChecksumCalculation = WhenRequired` 已设 |
| Worker 请求 502, error="source fetch failed" | Source 侧限流 (429) 或不可达 | driver 侧降 concurrency; 重试; 检查 source URL 有效性 |
| Restart 后所有 in-flight 消失 | Init 全部 Abort orphan (v1 预期行为) | 用户须重新提交. R2 lifecycle rule 会清理 orphan |
| VFS 里有条目但 R2 里没对象 | 用户手动删了 R2 对象 | Link 时 404 → driver warning log + VFS 删条目 |
| 下载 302 到 R2 报 403 | Presign expired (超过 24h) | 用户 refresh 页面重取 Link |
| 下载 custom_host 报 404 | custom_host 未在 CF 配置为 R2 public custom domain | 按 §14.3 步骤检查 |
| 长 PutURL 被反向代理断 | proxy timeout 太短 | 按 §16.5.3 调整 |
| SSRF 拒绝合法内网 host | 用户想代理内网源 (不该做) | 拒绝. 若确实需要 (企业内网), 手动改 driver.security.go 加白名单 |
| Multi bucket 覆盖同一 key | BucketsJSON 重复条目 | Init 有去重, 不该发生; 若发生, upgrade path 手动清 |
| Rename 后前端仍显示旧名 | OpenList dir cache 未失效 | driver Rename 内调 `op.Cache.dirCache.DeleteKey(...)` |
