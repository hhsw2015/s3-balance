package balancer

import "github.com/DullJZ/s3-balance/internal/bucket"

// Strategy 负载均衡策略接口
type Strategy interface {
	// SelectBucket 根据策略选择一个存储桶
	// buckets: 可用的存储桶列表
	// key: 对象的键
	// size: 对象的大小
	SelectBucket(buckets []*bucket.BucketInfo, key string, size int64) (*bucket.BucketInfo, error)
	
	// Name 返回策略名称
	Name() string
}
