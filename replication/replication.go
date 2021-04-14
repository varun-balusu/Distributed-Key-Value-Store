package replication

import (
	"bytes"
	"distribkv/usr/distributedkv/db"
	"encoding/json"
	"errors"
	"io/ioutil"
	"log"
	"net/http"
	"time"
)

type KeyValuePair struct {
	Key   string
	Value string
	Err   error
}

type LogEntry struct {
	Command db.Command
	Err     error
}

type ReplicationModule struct {
	commitIndex int
}

func KeyDownloadLoop(dba *db.Database, masterAddress string, myAddress string) {

	// time.Sleep(time.Second * 4)

	// RM := &ReplicationModule{commitIndex: -1}

	status := CheckMasterStatus(masterAddress)

	if status == "ok" {
		for {
			var url string = "http://" + myAddress + "/getCurrentClusterLeader"
			resp, err := http.Get(url)
			if err != nil {
				time.Sleep(time.Second * 2)
				continue
			}
			currentLeaderAddress, _ := ioutil.ReadAll(resp.Body)
			resp.Body.Close()
			if string(currentLeaderAddress) == myAddress {
				log.Printf("ending replication on server %v", myAddress)
				break
			} else {
				masterAddress = string(currentLeaderAddress)
				// log.Printf("my address is %v and my new leader is %v", myAddress, string(currentLeaderAddress))
			}
			// log.Printf("outside if-else the value of masterAddress is %v", masterAddress)
			keyfound, err := GetNextLogEntry(dba, masterAddress, myAddress)

			if err != nil {
				// log.Printf("my address from replication is %v", myAddress)
				log.Printf("Error Getting next log entry on server %v with: %v", myAddress, err)

				time.Sleep(time.Second * 2)
				continue
			}

			if !keyfound {
				// log.Printf("my address from replication is %v", myAddress)
				time.Sleep(time.Millisecond * 500)
			}
		}
	}

}

func CheckMasterStatus(masterAddress string) string {
	for {
		var url string = "http://" + masterAddress + "/fetchStatus"
		resp, err := http.Get(url)
		if err != nil {
			log.Printf("error in get skipping iteration")
			time.Sleep(time.Millisecond * 100)
			continue
		}

		status, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			log.Printf("error reading response body: %v", err)
		}
		// log.Println(string(status))
		if bytes.Equal(status, []byte("ok")) {
			resp.Body.Close()
			return string(status)
		}

		resp.Body.Close()
	}
}

func GetNextLogEntry(dba *db.Database, masterAddress string, myAddress string) (keyFound bool, err error) {
	//dynamically decide the master address maybe make another http request or import web package
	theLog := dba.GetLog()
	var currentLogEntry LogEntry
	var url string = "http://" + masterAddress + "/getNextLogEntry?address=" + myAddress

	resp, err := http.Get(url)
	if err != nil {
		return false, err
	}

	log.Printf("sent request to %v", masterAddress)
	// response, _ := ioutil.ReadAll(resp.Body)

	// log.Printf("the response body is %v", string(response))

	if err := json.NewDecoder(resp.Body).Decode(&currentLogEntry); err != nil {
		return false, err
	}

	if currentLogEntry.Err != nil {
		return false, currentLogEntry.Err
	} else {
		log.Printf("Current Log Entry Command is %+v and error is %v", currentLogEntry.Command, currentLogEntry.Err)
	}

	defer resp.Body.Close()
	//apply changes to replicas log here
	if err := dba.ExecuteCommand(currentLogEntry.Command); err != nil {
		log.Printf("error executing command on replica: %v", err)
		return false, err
	}
	////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////move it somehwere
	// latestIndex := strconv.Itoa(len(theLog.Transcript))
	// url = "http://" + masterAddress + "/confirmEntry?latestIndex=" + latestIndex

	// resp, err = http.Get(url)
	// if err != nil {
	// 	log.Printf("error confirming entry: %v", err)
	// 	return false, err
	// }
	// leaderCommitIndex, err := ioutil.ReadAll(resp.Body)
	// if err != nil {
	// 	log.Printf("error reading body for leader commit index: %v", err)
	// 	return false, err
	// }
	// resp.Body.Close()
	// // ignored error
	// leaderCommitIndexNum, _ := strconv.Atoi(string(leaderCommitIndex))
	// log.Printf("my address is %v and my leadersCommitIndex is %d", myAddress, leaderCommitIndexNum)
	// if leaderCommitIndexNum > RM.commitIndex {
	// 	log.Printf("my address is %v and im commting from %dto %d", myAddress, RM.commitIndex, leaderCommitIndexNum)
	// 	temp := RM.commitIndex
	// 	RM.commitIndex = leaderCommitIndexNum
	// 	go func(startIndex int, endIndex int) {
	// 		if startIndex == -1 {
	// 			startIndex = 0
	// 			RM.commitIndex = 0
	// 		}
	// 		for i := startIndex; i <= endIndex; i++ {
	// 			theLog := dba.TheLog
	// 			if i < len(theLog.Transcript) {
	// 				c := theLog.Transcript[i]
	// 				dba.ExecuteSetCommand(c)
	// 			} else {
	// 				break
	// 			}
	// 		}
	// 	}(temp, leaderCommitIndexNum)
	// }
	////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
	if err := RequestIncrement(masterAddress, dba, myAddress); err != nil {
		log.Printf("error sending confirmation and incrementing next index: %v", err)
	}

	log.Printf("the replicas log length is %d", len(theLog.Transcript))
	// time.Sleep(time.Second * 3)
	// log.Printf("my commit index at %v is %d", myAddress, RM.commitIndex)
	//true
	return true, nil
}

func RequestIncrement(masterAddress string, dba *db.Database, address string) error {

	var url string = "http://" + masterAddress + "/incrementNextIndex?address=" + address

	resp, err := http.Get(url)

	if err != nil {
		return err
	}

	status, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	defer resp.Body.Close()

	if !bytes.Equal(status, []byte("ok")) {
		return errors.New(string(status))
	}

	return nil
}
