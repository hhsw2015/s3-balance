package balancer

import (
	"fmt"
	"sync/atomic"

	"github.com/DullJZ/s3-balance/internal/bucket"
)

// RoundRobinStrategy 轮询策略
// 按照固定顺序循环选择存储桶
type RoundRobinStrategy struct {
	counter uint64
}

// NewRoundRobinStrategy 创建轮询策略
func NewRoundRobinStrategy() *RoundRobinStrategy {
	return &RoundRobinStrategy{}
}

// SelectBucket 选择存储桶（轮询）
// 使用原子操作确保并发安全
func (s *RoundRobinStrategy) SelectBucket(buckets []*bucket.BucketInfo, key string, size int64) (*bucket.BucketInfo, error) {
	if len(buckets) == 0 {
		return nil, fmt.Errorf("no buckets available")
	}
	
	// 原子递增计数器并取模，确保索引在有效范围内
	index := atomic.AddUint64(&s.counter, 1) % uint64(len(buckets))
	return buckets[index], nil
}

// Name 返回策略名称
func (s *RoundRobinStrategy) Name() string {
	return "round-robin"
}
