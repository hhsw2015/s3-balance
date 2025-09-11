package balancer

import (
	"fmt"
	"sort"

	"github.com/DullJZ/s3-balance/internal/bucket"
)

// LeastSpaceStrategy 最少使用空间策略
// 选择当前使用空间最少（可用空间最多）的存储桶
type LeastSpaceStrategy struct{}

// NewLeastSpaceStrategy 创建最少使用空间策略
func NewLeastSpaceStrategy() *LeastSpaceStrategy {
	return &LeastSpaceStrategy{}
}

// SelectBucket 选择存储桶（选择使用空间最少的）
// 优先选择可用空间最大的存储桶，有助于均衡存储使用
func (s *LeastSpaceStrategy) SelectBucket(buckets []*bucket.BucketInfo, key string, size int64) (*bucket.BucketInfo, error) {
	if len(buckets) == 0 {
		return nil, fmt.Errorf("no buckets available")
	}

	// 按可用空间排序（从大到小）
	// 使用稳定排序，保证相同可用空间的存储桶保持原有顺序
	sort.SliceStable(buckets, func(i, j int) bool {
		return buckets[i].GetAvailableSpace() > buckets[j].GetAvailableSpace()
	})

	// 返回可用空间最大的存储桶
	return buckets[0], nil
}

// Name 返回策略名称
func (s *LeastSpaceStrategy) Name() string {
	return "least-space"
}
