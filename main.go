package main

import (
	"distribkv/usr/distributedkv/config"
	"distribkv/usr/distributedkv/db"
	"distribkv/usr/distributedkv/deletion"
	"distribkv/usr/distributedkv/replication"
	"distribkv/usr/distributedkv/resharder"
	"distribkv/usr/distributedkv/web"
	"flag"
	"log"
	"net/http"
	"os"

	"github.com/BurntSushi/toml"
)

var (
	dblocation     = flag.String("db-location", "", "Path to the boltdb database")
	httpAddress    = flag.String("http-address", "127.0.0.1:8080", "HTTP host and port")
	configFile     = flag.String("config-file", "sharding-config.toml", "Path to static sharding configuration file")
	shard          = flag.String("shard", "", "current shard")
	reshardingMode = flag.Bool("resharding-mode", false, "Specifies whether to run in resharding mode or not")
	purge          = flag.Bool("purge", false, "Purge tells the db to delete extra keys that no longer belong to it")
	replica        = flag.Bool("replica", false, "Denotes whether or not to run the db in a read-only replica mode for the database shard that serves as the master")
)

func parseFlags() {
	flag.Parse()

	if *reshardingMode == true {
		log.Println("Entering Resharding Mode")
		return
	}

	if *dblocation == "" {
		log.Fatalln("Must Provide db-location")

	}

	if *shard == "" {
		log.Fatalln("Must Provide shard")
	}

}

func main() {

	parseFlags()

	if *reshardingMode == true {
		resharder.DoubleShards("/Users/vbalusu/Desktop/distribkv/sharding-config.toml", 1)
		path, err := os.Getwd()

		log.Printf("current path is %v and error is %v", path, err)
		return
	}

	db, closeDB, err := db.NewDatabase(*dblocation, *replica)
	defer closeDB()

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

	var replicaMap = make(map[int][]string)

	for _, s := range config.Shards {

		addressMap[s.Index] = s.Address

		replicaMap[s.Index] = s.Replicas

		if s.Name == *shard {

			shardIndex = s.Index

		}
	}

	if shardIndex < 0 {
		log.Fatalf("Shard %q was not found", *shard)
	}

	//delete extra keys offline if purge flag is set to true
	if *purge == true {
		db.DeleteExtraKeys(shardIndex, shardCount)
		return
	}

	if *replica {
		masterAddress, present := addressMap[shardIndex]

		if !present {
			log.Fatalf("Could not find a address mapping for master shard with index %d", shardIndex)
		}

		go replication.KeyDownloadLoop(db, masterAddress)
		go deletion.KeyDeletionLoop(db, masterAddress)
	}

	log.Printf("Current shard is %q and shard index is %v and total shard count is %v", *shard, shardIndex, shardCount)
	var srv *web.Server
	if !*replica {
		srv = web.NewServer(db, shardIndex, shardCount, addressMap, replicaMap[shardIndex])
	} else {
		srv = web.NewServer(db, shardIndex, shardCount, addressMap, nil)
	}

	http.HandleFunc("/get", srv.HandleGet)

	http.HandleFunc("/set", srv.HandleSet)

	http.HandleFunc("/delete", srv.HandleDelete)

	http.HandleFunc("/purge", srv.HandleDeleteExtraKeys)

	http.HandleFunc("/getReplicationHead", srv.HandleReplicationQueueHead)

	http.HandleFunc("/deleteKeyFRQ", srv.HandleDeleteKeyFromReplicationQueue)

	http.HandleFunc("/deleteKeyFDQ", srv.HandleDeleteKeyFromDeletionQueue)

	http.HandleFunc("/getDeletionHead", srv.HandleDeletionQueueHead)

	log.Fatal(http.ListenAndServe(*httpAddress, nil))

}
