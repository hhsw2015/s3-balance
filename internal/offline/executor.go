package offline

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/DullJZ/s3-balance/internal/bucket"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// Config wires the Cloudflare Worker used as a byte pump.
type Config struct {
	WorkerBaseURL string
	WorkerAuth    string
	PartSize      int64
	Concurrency   int
}

// Executor drives one task by talking to a stateless Worker.
// s3-balance keeps the whole state machine in-memory (PoC).
type Executor struct {
	cfg    Config
	store  *Store
	bkts   *bucket.Manager
	client *http.Client
}

func NewExecutor(cfg Config, store *Store, bkts *bucket.Manager) *Executor {
	if cfg.PartSize == 0 {
		cfg.PartSize = 95 * 1024 * 1024
	}
	if cfg.Concurrency == 0 {
		cfg.Concurrency = 2
	}
	return &Executor{
		cfg:   cfg,
		store: store,
		bkts:  bkts,
		client: &http.Client{
			Timeout: 120 * time.Second,
		},
	}
}

type headResp struct {
	OK            bool   `json:"ok"`
	Status        int    `json:"status"`
	ContentLength string `json:"contentLength"`
	AcceptRanges  string `json:"acceptRanges"`
}

type partReq struct {
	SourceURL  string `json:"source_url"`
	Key        string `json:"key"`
	UploadID   string `json:"uploadId"`
	PartNumber int    `json:"partNumber"`
	RangeStart int64  `json:"rangeStart"`
	RangeEnd   int64  `json:"rangeEnd"`
}

type partResp struct {
	PartNumber int    `json:"partNumber"`
	ETag       string `json:"etag"`
	Error      string `json:"error"`
}

// Submit registers the task and kicks off an async driver goroutine.
func (e *Executor) Submit(t *Task) (*Task, error) {
	size, err := e.probeSize(t.SourceURL)
	if err != nil {
		return nil, fmt.Errorf("probe size: %w", err)
	}
	t.Size = size
	t.PartSize = e.cfg.PartSize
	t.PartsTotal = int((size + t.PartSize - 1) / t.PartSize)
	t.Status = StatusPending
	t.CreatedAt = time.Now()
	e.store.Put(t)

	go e.drive(t.ID)

	got, _ := e.store.Get(t.ID)
	return got, nil
}

func (e *Executor) probeSize(sourceURL string) (int64, error) {
	// try HEAD via Worker, retry a few times (source may 429/520 transiently)
	var lastErr error
	for attempt := 0; attempt < 4; attempt++ {
		size, err := e.probeSizeOnce(sourceURL)
		if err == nil {
			return size, nil
		}
		lastErr = err
		time.Sleep(time.Duration(2*(attempt+1)) * time.Second)
	}
	return 0, lastErr
}

func (e *Executor) probeSizeOnce(sourceURL string) (int64, error) {
	body, _ := json.Marshal(map[string]string{"source_url": sourceURL})
	req, _ := http.NewRequest(http.MethodPost, e.cfg.WorkerBaseURL+"/head", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Auth", e.cfg.WorkerAuth)
	resp, err := e.client.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	buf, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("worker /head status %d: %s", resp.StatusCode, string(buf))
	}
	var h headResp
	if err := json.Unmarshal(buf, &h); err != nil {
		return 0, err
	}
	if !h.OK {
		return 0, fmt.Errorf("source HEAD not ok: status %d", h.Status)
	}
	if h.AcceptRanges != "bytes" {
		return 0, fmt.Errorf("source does not support Range")
	}
	var size int64
	if _, err := fmt.Sscanf(h.ContentLength, "%d", &size); err != nil {
		return 0, fmt.Errorf("parse content-length %q: %w", h.ContentLength, err)
	}
	if size <= 0 {
		return 0, fmt.Errorf("invalid size %d", size)
	}
	return size, nil
}

// drive is the whole state machine: init, dispatch parts concurrently, complete.
func (e *Executor) drive(taskID string) {
	t, ok := e.store.Get(taskID)
	if !ok {
		log.Printf("[offline %s] task missing", taskID)
		return
	}

	// pick the R2 client
	b, ok := e.bkts.GetBucket(t.R2Bucket)
	if !ok {
		e.fail(taskID, "r2 bucket %q not found", t.R2Bucket)
		return
	}
	s3c := b.Client
	ctx := context.Background()

	// InitiateMultipartUpload
	initOut, err := s3c.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
		Bucket: aws.String(b.Config.Name),
		Key:    aws.String(t.Key),
	})
	if err != nil {
		e.fail(taskID, "createMultipartUpload: %v", err)
		return
	}
	uploadID := aws.ToString(initOut.UploadId)
	log.Printf("[offline %s] uploadId=%s parts=%d", taskID, uploadID, t.PartsTotal)

	e.store.Update(taskID, func(x *Task) { x.Status = StatusRunning })

	// dispatch parts across a goroutine pool
	type partOK struct {
		N    int
		ETag string
	}
	results := make(chan partOK, t.PartsTotal)
	errs := make(chan error, t.PartsTotal)
	sem := make(chan struct{}, e.cfg.Concurrency)
	var wg sync.WaitGroup

	for n := 1; n <= t.PartsTotal; n++ {
		wg.Add(1)
		go func(partN int) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()

			start := int64(partN-1) * t.PartSize
			end := start + t.PartSize - 1
			if end >= t.Size {
				end = t.Size - 1
			}

			var lastErr error
			for attempt := 0; attempt < 5; attempt++ {
				etag, err := e.uploadPartViaWorker(t.SourceURL, t.R2Bucket, t.Key, uploadID, partN, start, end)
				if err == nil {
					results <- partOK{N: partN, ETag: etag}
					e.store.Update(taskID, func(x *Task) { x.PartsDone++ })
					return
				}
				lastErr = err
				log.Printf("[offline %s] part %d attempt %d: %v", taskID, partN, attempt+1, err)
				time.Sleep(time.Duration(2*(attempt+1)) * time.Second)
			}
			errs <- fmt.Errorf("part %d: %w", partN, lastErr)
		}(n)
	}
	wg.Wait()
	close(results)
	close(errs)

	if len(errs) > 0 {
		firstErr := <-errs
		// abort multipart upload on any part failure
		_, _ = s3c.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{
			Bucket:   aws.String(b.Config.Name),
			Key:      aws.String(t.Key),
			UploadId: aws.String(uploadID),
		})
		e.fail(taskID, "%v", firstErr)
		return
	}

	// collect + sort etags
	parts := make([]s3types.CompletedPart, 0, t.PartsTotal)
	etagMap := map[int]string{}
	for r := range results {
		etagMap[r.N] = r.ETag
	}
	for n := 1; n <= t.PartsTotal; n++ {
		etag, ok := etagMap[n]
		if !ok {
			e.fail(taskID, "missing etag for part %d", n)
			return
		}
		parts = append(parts, s3types.CompletedPart{
			ETag:       aws.String(etag),
			PartNumber: aws.Int32(int32(n)),
		})
	}

	// CompleteMultipartUpload
	completeOut, err := s3c.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket:          aws.String(b.Config.Name),
		Key:             aws.String(t.Key),
		UploadId:        aws.String(uploadID),
		MultipartUpload: &s3types.CompletedMultipartUpload{Parts: parts},
	})
	if err != nil {
		e.fail(taskID, "completeMultipartUpload: %v", err)
		return
	}

	e.store.Update(taskID, func(x *Task) {
		x.Status = StatusDone
		x.ETag = aws.ToString(completeOut.ETag)
	})
	log.Printf("[offline %s] done, etag=%s", taskID, aws.ToString(completeOut.ETag))
}

func (e *Executor) uploadPartViaWorker(sourceURL, r2Bucket, key, uploadID string, partN int, rangeStart, rangeEnd int64) (string, error) {
	_ = r2Bucket // Worker binding fixes the R2 bucket; kept in signature for future multi-bucket
	body, _ := json.Marshal(partReq{
		SourceURL:  sourceURL,
		Key:        key,
		UploadID:   uploadID,
		PartNumber: partN,
		RangeStart: rangeStart,
		RangeEnd:   rangeEnd,
	})
	req, _ := http.NewRequest(http.MethodPost, e.cfg.WorkerBaseURL+"/part", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Auth", e.cfg.WorkerAuth)
	resp, err := e.client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	buf, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("worker /part status %d: %s", resp.StatusCode, string(buf))
	}
	var p partResp
	if err := json.Unmarshal(buf, &p); err != nil {
		return "", err
	}
	if p.Error != "" {
		return "", fmt.Errorf("worker: %s", p.Error)
	}
	if p.ETag == "" {
		return "", fmt.Errorf("empty etag")
	}
	return p.ETag, nil
}

func (e *Executor) fail(id, format string, args ...any) {
	msg := fmt.Sprintf(format, args...)
	log.Printf("[offline %s] FAIL: %s", id, msg)
	e.store.Update(id, func(t *Task) {
		t.Status = StatusFailed
		t.Error = msg
	})
}
