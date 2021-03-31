package deletion

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

func KeyDeletionLoop(db *db.Database, masterAddress string) {

	for {

		keyfound, err := fetchKeys(db, masterAddress)

		if err != nil {
			log.Printf("Error deleting keys: %v", err)
			time.Sleep(time.Second)
			continue
		}

		if !keyfound {
			time.Sleep(time.Millisecond * 100)
		}

	}
}

func deleteKeys(db *db.Database, masterAddress string) (err error) {
	var url string = "http://" + masterAddress + "/getDeletionHead"

	resp, err := http.Get(url)

	if err != nil {
		return err
	}

	var response KeyValuePair

	if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
		return err
	}

	if response.Err != nil {
		return err
	}

}

func deleteKeyFromReplicationQueue(key string, value string, masterAddress string) (err error) {

	var url string = "http://" + masterAddress + "/deleteKeyFRQ?" + "key=" + key + "&value=" + value

	log.Printf("deleting key %v with value %v on server %v", key, value, masterAddress)

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
