package balancer

import (
	"fmt"
	"math/rand"
	"sync"

	"github.com/DullJZ/s3-balance/internal/bucket"
)

// WeightedStrategy 加权策略
// 根据存储桶配置的权重进行选择，权重越高被选中的概率越大
type WeightedStrategy struct {
	mu sync.RWMutex
}

// NewWeightedStrategy 创建加权策略
func NewWeightedStrategy() *WeightedStrategy {
	return &WeightedStrategy{}
}

// SelectBucket 选择存储桶（基于权重）
// 权重越高的存储桶被选中的概率越大
func (s *WeightedStrategy) SelectBucket(buckets []*bucket.BucketInfo, key string, size int64) (*bucket.BucketInfo, error) {
	if len(buckets) == 0 {
		return nil, fmt.Errorf("no buckets available")
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	// 计算总权重
	totalWeight := 0
	for _, b := range buckets {
		totalWeight += b.Config.Weight
	}

	if totalWeight == 0 {
		// 如果所有权重都是0，则随机选择
		return buckets[rand.Intn(len(buckets))], nil
	}

	// 根据权重随机选择
	// 生成一个 [0, totalWeight) 范围内的随机数
	randomWeight := rand.Intn(totalWeight)
	currentWeight := 0

	// 遍历存储桶，累加权重直到超过随机数
	for _, b := range buckets {
		currentWeight += b.Config.Weight
		if randomWeight < currentWeight {
			return b, nil
		}
	}

	// 不应该到达这里，但为了安全返回最后一个
	return buckets[len(buckets)-1], nil
}

// Name 返回策略名称
func (s *WeightedStrategy) Name() string {
	return "weighted"
}
