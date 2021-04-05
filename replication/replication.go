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

func KeyDownloadLoop(dba *db.Database, masterAddress string, myAddress string) {

	// time.Sleep(time.Second * 4)

	status := CheckMasterStatus(masterAddress)

	if status == "ok" {
		for {
			keyfound, err := GetNextLogEntry(dba, masterAddress, myAddress)

			if err != nil {
				log.Printf("Error Getting next log entry: %v", err)
				time.Sleep(time.Second * 2)
				continue
			}

			if !keyfound {
				time.Sleep(time.Millisecond * 100)
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
	theLog := dba.GetLog()
	var currentLogEntry LogEntry
	var url string = "http://" + masterAddress + "/getNextLogEntry?address=" + myAddress

	resp, err := http.Get(url)
	if err != nil {
		return false, err
	}

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

	if err := RequestIncrement(masterAddress, dba, myAddress); err != nil {
		log.Printf("error sending confirmation and incrementing next index: %v", err)
	}

	log.Printf("the replicas log length is %d", len(theLog.Transcript))
	time.Sleep(time.Second * 3)

	//true
	return false, nil
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

// func KeyDownloadLoop(db *db.Database, masterAddress string) {

// 	for {

// 		keyfound, err := fetchKeys(db, masterAddress)

// 		if err != nil {
// 			log.Printf("Error fetching keys: %v", err)
// 			time.Sleep(time.Second)
// 			continue
// 		}

// 		if !keyfound {
// 			time.Sleep(time.Millisecond * 100)
// 		}

// 	}
// }

// func fetchKeys(db *db.Database, masterAddress string) (keyfound bool, err error) {
// 	var url string = "http://" + masterAddress + "/getReplicationHead"
// 	resp, err := http.Get(url)

// 	if err != nil {
// 		return false, err
// 	}
// 	var response KeyValuePair

// 	if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
// 		return false, err
// 	}

// 	// body, err := ioutil.ReadAll(resp.Body)
// 	// if err := json.Unmarshal(body, &response); err != nil {
// 	// 	return false, err
// 	// }

// 	defer resp.Body.Close()

// 	if response.Err != nil {
// 		return false, err
// 	}

// 	if response.Key == "" {
// 		return false, nil
// 	}

// 	if err := db.SetReplicationKey(string(response.Key), []byte(response.Value)); err != nil {
// 		return false, err
// 	}

// 	if err := deleteKeyFromReplicationQueue(string(response.Key), string(response.Value), masterAddress); err != nil {
// 		log.Printf("delete key from replica queue failed: %v", err)
// 	}

// 	//change to true
// 	return false, nil
// }

// func deleteKeyFromReplicationQueue(key string, value string, masterAddress string) (err error) {

// 	var url string = "http://" + masterAddress + "/deleteKeyFRQ?" + "key=" + key + "&value=" + value

// 	log.Printf("deleting key %v with value %v on server %v from replication.go", key, value, masterAddress)

// 	resp, err := http.Get(url)

// 	if err != nil {
// 		return err
// 	}

// 	defer resp.Body.Close()

// 	status, err := ioutil.ReadAll(resp.Body)
// 	if err != nil {
// 		return err
// 	}

// 	if !bytes.Equal(status, []byte("ok")) {
// 		return errors.New(string(status))
// 	}

// 	return nil

// }
