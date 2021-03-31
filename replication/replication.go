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

func KeyDownloadLoop(db *db.Database, masterAddress string) {

	for {

		keyfound, err := fetchKeys(db, masterAddress)

		if err != nil {
			log.Printf("Error fetching keys: %v", err)
			time.Sleep(time.Second)
			continue
		}

		if !keyfound {
			time.Sleep(time.Millisecond * 100)
		}

	}
}

func fetchKeys(db *db.Database, masterAddress string) (keyfound bool, err error) {
	var url string = "http://" + masterAddress + "/getReplicationHead"
	resp, err := http.Get(url)

	if err != nil {
		return false, err
	}
	var response KeyValuePair

	if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
		return false, err
	}

	// body, err := ioutil.ReadAll(resp.Body)
	// if err := json.Unmarshal(body, &response); err != nil {
	// 	return false, err
	// }

	defer resp.Body.Close()

	if response.Err != nil {
		return false, err
	}

	if response.Key == "" {
		return false, nil
	}

	if err := db.SetReplicationKey(string(response.Key), []byte(response.Value)); err != nil {
		return false, err
	}

	if err := deleteKeyFromReplicationQueue(string(response.Key), string(response.Value), masterAddress); err != nil {
		log.Printf("delete key from replica queue failed: %v", err)
	}

	//change to true
	return false, nil
}

func deleteKeyFromReplicationQueue(key string, value string, masterAddress string) (err error) {

	var url string = "http://" + masterAddress + "/deleteKeyFRQ?" + "key=" + key + "&value=" + value

	log.Printf("deleting key %v with value %v on server %v from replication.go", key, value, masterAddress)

	resp, err := http.Get(url)

	if err != nil {
		return err
	}

	defer resp.Body.Close()

	status, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	if !bytes.Equal(status, []byte("ok")) {
		return errors.New(string(status))
	}

	return nil

}
