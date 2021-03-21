package main

import (
	"distribkv/usr/distributedkv/config"
	"distribkv/usr/distributedkv/db"
	"distribkv/usr/distributedkv/web"
	"flag"
	"log"
	"net/http"

	"github.com/BurntSushi/toml"
)

var (
	dblocation  = flag.String("db-location", "", "Path to the boltdb database")
	httpAddress = flag.String("http-address", "127.0.0.1:8080", "HTTP host and port")
	configFile  = flag.String("config-file", "sharding-config.toml", "Path to static sharding configuration file")
	shard       = flag.String("shard", "", "current shard")
)

func parseFlags() {
	flag.Parse()

	if *dblocation == "" {
		log.Fatalln("Must Provide db-location")

	}

	if *shard == "" {
		log.Fatalln("Must Provide shard")
	}
}

func main() {

	parseFlags()

	db, closeDB, err := db.NewDatabase(*dblocation)

	if err != nil {
		log.Fatalf("NewDatabase(%q): failed with error: %v", *dblocation, err)
	}

	var config config.Config

	if _, err := toml.DecodeFile(*configFile, &config); err != nil {
		log.Fatalf("toml.DecodeFile(%q): %v", *configFile, err)
	}

	var shardIndex int = -1

	var shardCount int = len(config.Shards)

	var addressMap = make(map[int]string)

	for _, s := range config.Shards {

		addressMap[s.Index] = s.Address

		if s.Name == *shard {

			shardIndex = s.Index

		}
	}

	if shardIndex < 0 {
		log.Fatalf("Shard %q was not found", *shard)
	}

	log.Printf("Current shard is %q and shard index is %v and total shard count is %v", *shard, shardIndex, shardCount)

	defer closeDB()

	srv := web.NewServer(db, shardIndex, shardCount, addressMap)

	http.HandleFunc("/get", srv.HandleGet)

	http.HandleFunc("/set", srv.HandleSet)

	http.HandleFunc("/delete", srv.HandleDelete)

	log.Fatal(http.ListenAndServe(*httpAddress, nil))

}
