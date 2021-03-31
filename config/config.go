package config

type Shard struct {
	Name     string
	Index    int
	Address  string
	Replicas []string
}

type Config struct {
	Shards []Shard
}
