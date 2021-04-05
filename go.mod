module distribkv/usr/distributedkv

//PATH=$PATH:$(dirname $(go list -f '{{.Target}}' .))

go 1.16

require (
	github.com/BurntSushi/toml v0.3.1 // direct
	go.etcd.io/bbolt v1.3.5 // direct
	golang.org/x/sync v0.0.0-20210220032951-036812b2e83c // indirect
)
