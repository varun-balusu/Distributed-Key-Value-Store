package hearbeat

import (
	"distribkv/usr/distributedkv/db"
	"encoding/json"
	"errors"
	"io/ioutil"
	"log"
	"net/http"
	"strconv"
	"sync"
	"time"
)

type LeaderList struct {
	LeaderAddresses []string
}

// SendHeartbeats sends heartbeats to all replicas in the replicaArr every 50 milliseconds
// if isnewLeader is set to true it will initialize a new leader if dba is not null
func SendHeartbeats(replicaArr []string, isnewLeader bool, leaderAddress string, dba *db.Database) {

	if isnewLeader {
		err := InitLeader(leaderAddress, dba, replicaArr)
		if err != nil {
			log.Printf("error initializing the leader node %v", err)
		}

	}

	duration := time.Duration(50) * time.Millisecond

	for i := 0; i < len(replicaArr); i++ {
		log.Printf("my address is %v and im sending heartbeats to %v", leaderAddress, replicaArr[i])
	}

	tk := time.NewTicker(duration)
	for range tk.C {

		for i := 0; i < len(replicaArr); i++ {
			err := TriggerHeartbeat(replicaArr[i])
			if err != nil {
				// log.Printf("Error sending heartbeats: %v", err)
				// time.Sleep(time.Second * 2)
			}
		}

	}

	defer tk.Stop()

}

// TriggerHeatbeat sends a request to requested address, by htting the triggetHeartbeat endpoint
// this asserts the leaders authority by reseting the election timeouts on the server with specified address.
func TriggerHeartbeat(address string) error {
	var url string = "http://" + address + "/triggerHeartbeat"

	resp, err := http.Get(url)
	if err != nil {
		// log.Printf("error sending heartbeats: %v", err)
		return err
	}

	status, err := ioutil.ReadAll(resp.Body)

	if err != nil {
		// log.Printf("error parsing body: %v", err)
		return err
	} else if string(status) == "ok" {
		// log.Printf("got ack from replica with status: %v", string(status))
	}

	resp.Body.Close()

	return nil
}

// InitLeader gets called when a new leader wins an election. By initializing a new leader the leader address gets
// sent to all current leaders including the elected leader. Each leader updates it address map to point at the newly
// elected leader as leader of the shard cluster. The new leader also communicated with the replicas in its cluster to
// update its log Index Map so it can correctly continue replicating to replicas.
func InitLeader(leaderAddress string, dba *db.Database, replicaArr []string) error {
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
	resp.Body.Close()
	log.Printf("shard index is %v", string(shardIndex))

	var leaders LeaderList
	resp, err = http.Get("http://" + leaderAddress + "/getLeadersList?newLeaderIdx=" + string(shardIndex))
	if err != nil {
		log.Printf("error getting leader list %v", err)
		return err
	}
	json.NewDecoder(resp.Body).Decode(&leaders)
	resp.Body.Close()
	leaders.LeaderAddresses = append(leaders.LeaderAddresses, leaderAddress)
	for i := 0; i < len(leaders.LeaderAddresses); i++ {
		log.Printf(leaders.LeaderAddresses[i])
	}

	dba.ReadOnly = false
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
				wg.Done()
				return
			}

			status, err := ioutil.ReadAll(resp.Body)
			if err != nil {
				errHandler = err
			}
			if string(status) != "ok" {
				errHandler = errors.New("error modifying the address map at url: " + url)
				// log.Printf("uh-oh")
			}
			resp.Body.Close()
			wg.Done()
		}(url)

		// err := <-errHandler
		// if err != nil {
		// 	return err
		// }
	}
	wg.Wait()
	log.Printf("done with modify loop")
	if errHandler.Error() != "" {
		return errHandler
		// log.Printf(errHandler.Error())
	}

	//we also have to intialize a leader by updating its index map so it can correctly continue to
	// replicate log entries to other replicas
	for i := 0; i < len(replicaArr); i++ {
		// if replicaArr[i] != "127.0.0.3:8080" {
		go func(address string, dba *db.Database) {
			var url string = "http://" + address + "/getLogLength"
			resp, err := http.Get(url)
			if err != nil {
				log.Printf("there was an error getting replica log length at url %v", url)
				return
			}
			logLength, err := ioutil.ReadAll(resp.Body)
			if err != nil {
				log.Printf("error reading the body when getting log length %v", err)
				return
			}
			resp.Body.Close()
			logLengthAsNumber, _ := strconv.Atoi(string(logLength))

			if logLengthAsNumber == 0 {
				dba.IndexMap[address] = logLengthAsNumber
				return
			}

			dba.IndexMap[address] = logLengthAsNumber - 1

		}(replicaArr[i], dba)
		// }

	}

	return nil
}
