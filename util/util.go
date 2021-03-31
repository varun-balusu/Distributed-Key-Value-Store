package util

import "hash/fnv"

func IsExtraKey(key string, currentShardIndex int, shardCount int) bool {
	hash := fnv.New64()

	hash.Write([]byte(key))

	shardIdx := int(hash.Sum64() % uint64(shardCount))

	if currentShardIndex != shardIdx {
		return true
	} else {
		return false
	}
}
