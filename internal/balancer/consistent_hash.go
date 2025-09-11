package balancer

import (
	"crypto/md5"
	"encoding/binary"
	"fmt"
	"sort"
	"sync"

	"github.com/DullJZ/s3-balance/internal/bucket"
)

// ConsistentHashStrategy 一致性哈希策略
// 使用一致性哈希算法，确保相同的key总是映射到相同的存储桶
// 当存储桶增加或减少时，能够最小化数据迁移
type ConsistentHashStrategy struct {
	replicas int                           // 每个节点的虚拟节点数
	ring     map[uint32]*bucket.BucketInfo // 哈希环
	nodes    []uint32                      // 排序的哈希值列表
	mu       sync.RWMutex                  // 读写锁，保护并发访问
}

// NewConsistentHashStrategy 创建一致性哈希策略
func NewConsistentHashStrategy() *ConsistentHashStrategy {
	return &ConsistentHashStrategy{
		replicas: 100, // 每个节点的虚拟节点数，越大分布越均匀
		ring:     make(map[uint32]*bucket.BucketInfo),
	}
}

// SelectBucket 选择存储桶（基于一致性哈希）
// 相同的key总是会映射到相同的存储桶
func (s *ConsistentHashStrategy) SelectBucket(buckets []*bucket.BucketInfo, key string, size int64) (*bucket.BucketInfo, error) {
	if len(buckets) == 0 {
		return nil, fmt.Errorf("no buckets available")
	}

	// 更新哈希环
	s.updateRing(buckets)

	// 计算key的哈希值
	hash := s.hash(key)

	// 在环上找到第一个大于等于hash的节点
	s.mu.RLock()
	defer s.mu.RUnlock()

	// 二分查找第一个大于等于hash的节点
	idx := sort.Search(len(s.nodes), func(i int) bool {
		return s.nodes[i] >= hash
	})

	// 如果没找到，返回第一个节点（环形结构）
	if idx == len(s.nodes) {
		idx = 0
	}

	return s.ring[s.nodes[idx]], nil
}

// updateRing 更新哈希环
// 为每个存储桶创建多个虚拟节点，使哈希分布更均匀
func (s *ConsistentHashStrategy) updateRing(buckets []*bucket.BucketInfo) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// 清空现有环
	s.ring = make(map[uint32]*bucket.BucketInfo)
	s.nodes = nil

	// 为每个存储桶添加虚拟节点
	for _, b := range buckets {
		for i := 0; i < s.replicas; i++ {
			// 为每个虚拟节点生成唯一的key
			virtualKey := fmt.Sprintf("%s-%d", b.Config.Name, i)
			hash := s.hash(virtualKey)
			s.ring[hash] = b
			s.nodes = append(s.nodes, hash)
		}
	}

	// 排序节点，便于二分查找
	sort.Slice(s.nodes, func(i, j int) bool {
		return s.nodes[i] < s.nodes[j]
	})
}

// hash 计算哈希值
// 使用MD5算法，取前4个字节作为uint32
func (s *ConsistentHashStrategy) hash(key string) uint32 {
	h := md5.Sum([]byte(key))
	return binary.BigEndian.Uint32(h[:4])
}

// Name 返回策略名称
func (s *ConsistentHashStrategy) Name() string {
	return "consistent-hash"
}

// SetReplicas 设置虚拟节点数
// 虚拟节点越多，负载分布越均匀，但内存使用也越多
func (s *ConsistentHashStrategy) SetReplicas(replicas int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	
	if replicas > 0 {
		s.replicas = replicas
		// 清空现有环，下次选择时会重新构建
		s.ring = make(map[uint32]*bucket.BucketInfo)
		s.nodes = nil
	}
}
