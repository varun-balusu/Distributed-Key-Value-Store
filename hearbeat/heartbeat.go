package hearbeat

import (
	"distribkv/usr/distributedkv/db"
	"encoding/json"
	"errors"
	"io/ioutil"
	"log"
	"net/http"
	"sync"
	"time"
)

type LeaderList struct {
	LeaderAddresses []string
}

func SendHeartbeats(replicaArr []string, isnewLeader bool, leaderAddress string, db *db.Database) {

	if isnewLeader {
		err := InitLeader(leaderAddress, db, replicaArr)
		if err != nil {
			log.Printf("error initializing the leader node %v", err)
		}

	}

	duration := time.Duration(50) * time.Millisecond

	tk := time.NewTicker(duration)
	for range tk.C {

		for i := 0; i < len(replicaArr); i++ {
			TriggerHeartbeat(replicaArr[i])
		}

	}

	defer tk.Stop()

}

func TriggerHeartbeat(address string) {
	var url string = "http://" + address + "/triggerHeartbeat"

	resp, err := http.Get(url)
	if err != nil {
		log.Printf("error sending heartbeats: %v", err)
	}

	status, err := ioutil.ReadAll(resp.Body)

	if err != nil {
		log.Printf("error parsing body: %v", err)
	} else if string(status) == "ok" {
		// log.Printf("got ack from replica with status: %v", string(status))
	}

	defer resp.Body.Close()
}

func InitLeader(leaderAddress string, db *db.Database, replicaArr []string) error {
	resp, err := http.Get("http://" + leaderAddress + "/getShardIndex")
	if err != nil {
		log.Printf("error getting shard index %v", err)
		return err
	}
	shardIndex, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Printf("error parsing body %v", err)
		return err
	}

	log.Printf("shard index is %v", string(shardIndex))

	var leaders LeaderList
	resp, err = http.Get("http://" + leaderAddress + "/getLeadersList?newLeaderIdx=" + string(shardIndex))
	if err != nil {
		log.Printf("error getting leader list %v", err)
		return err
	}
	json.NewDecoder(resp.Body).Decode(&leaders)

	leaders.LeaderAddresses = append(leaders.LeaderAddresses, leaderAddress)
	for i := 0; i < len(leaders.LeaderAddresses); i++ {
		log.Printf(leaders.LeaderAddresses[i])
	}

	db.ReadOnly = false
	var wg sync.WaitGroup
	log.Printf("lenght of the array is: %d", len(leaders.LeaderAddresses))
	errHandler := errors.New("")
	for i := 0; i < len(leaders.LeaderAddresses); i++ {

		var url string = "http://" + leaders.LeaderAddresses[i] + "/modifyAddressMap?shardIndex=" + string(shardIndex) + "&newLeader=" + leaderAddress
		// log.Printf("making request to url: %v", url)

		wg.Add(1)
		go func(url string) {
			resp, err := http.Get(url)
			if err != nil {
				errHandler = err
			}

			status, err := ioutil.ReadAll(resp.Body)
			if err != nil {
				errHandler = err
			}
			if string(status) != "ok" {
				errHandler = errors.New("error modifying the address map at url: " + url)
				// log.Printf("uh-oh")
			}

			wg.Done()
		}(url)

		// err := <-errHandler
		// if err != nil {
		// 	return err
		// }
	}
	wg.Wait()
	if errHandler.Error() != "" {
		return errHandler
	}

	return nil
}
