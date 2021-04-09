package main

import (
	"distribkv/usr/distributedkv/config"
	"distribkv/usr/distributedkv/db"
	"distribkv/usr/distributedkv/election"
	"distribkv/usr/distributedkv/hearbeat"
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

func filterOutAddress(replicaArr []string, httpAddress string) []string {
	var res []string

	for i := 0; i < len(replicaArr); i++ {
		if replicaArr[i] != httpAddress {
			res = append(res, replicaArr[i])
		}
	}

	return res
}

func main() {

	parseFlags()

	if *reshardingMode == true {
		resharder.DoubleShards("/Users/vbalusu/Desktop/distribkv/sharding-config.toml", 1)
		path, err := os.Getwd()

		log.Printf("current path is %v and error is %v", path, err)
		return
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

	db, closeDB, err := db.NewDatabase(*dblocation, *replica, replicaMap[shardIndex])

	defer closeDB()

	if err != nil {
		log.Fatalf("NewDatabase(%q): failed with error: %v", *dblocation, err)
	}

	//delete extra keys offline if purge flag is set to true
	if *purge == true {
		db.DeleteExtraKeys(shardIndex, shardCount)
		return
	}

	masterAddress, present := addressMap[shardIndex]

	if !present {
		log.Fatalf("Could not find a address mapping for master shard with index %d", shardIndex)
	}

	if *replica {

		go replication.KeyDownloadLoop(db, masterAddress, *httpAddress)

		filteredAddresses := filterOutAddress(replicaMap[shardIndex], *httpAddress)
		numNodes := len(replicaMap[shardIndex]) + 1
		go election.ElectionLoop(filteredAddresses, numNodes, *httpAddress, db)

	} else {
		go hearbeat.SendHeartbeats(replicaMap[shardIndex], false, masterAddress, db)
	}

	log.Printf("Current shard is %q and shard index is %v and total shard count is %v", *shard, shardIndex, shardCount)
	var srv *web.Server
	if !*replica {
		srv = web.NewServer(db, shardIndex, shardCount, addressMap, replicaMap[shardIndex])
	} else {
		filteredAddresses := filterOutAddress(replicaMap[shardIndex], *httpAddress)
		srv = web.NewServer(db, shardIndex, shardCount, addressMap, filteredAddresses)
	}

	http.HandleFunc("/get", srv.HandleGet)

	http.HandleFunc("/set", srv.HandleSet)

	http.HandleFunc("/delete", srv.HandleDelete)

	http.HandleFunc("/purge", srv.HandleDeleteExtraKeys)

	http.HandleFunc("/readLog", srv.HandleReadLog)

	http.HandleFunc("/getLogAtIndex", srv.HandleFetchLogIndex)

	http.HandleFunc("/getNextLogEntry", srv.HandleGetNextLogEntry)

	http.HandleFunc("/fetchStatus", srv.FetchStatus)

	http.HandleFunc("/getLogLength", srv.HandleGetLogLength)

	http.HandleFunc("/incrementNextIndex", srv.HandleIncrementNextIndex)

	http.HandleFunc("/triggerHeartbeat", srv.HandleTriggerHeartbeat)

	http.HandleFunc("/triggerVoteRequest", srv.HandleTriggerVoteRequest)

	http.HandleFunc("/triggerVoteForSelf", srv.HandleVoteForSelf)

	http.HandleFunc("/triggerNextTerm", srv.HandleTriggerNextTerm)

	http.HandleFunc("/getCurrentTerm", srv.HandleFetchCurrentTerm)

	http.HandleFunc("/getShardIndex", srv.GetShardIndex)

	http.HandleFunc("/getLeadersList", srv.GetLeaderAddresses)

	http.HandleFunc("/modifyAddressMap", srv.ModifyAddressMap)

	http.HandleFunc("/getCurrentClusterLeader", srv.GetCurrentClusterLeader)

	log.Fatal(http.ListenAndServe(*httpAddress, nil))

}
