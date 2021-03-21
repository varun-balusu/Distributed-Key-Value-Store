package config

type Shard struct {
	Name    string
	Index   int
	Address string
}

type Config struct {
	Shards []Shard
}
