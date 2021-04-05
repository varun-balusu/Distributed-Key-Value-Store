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

func KeyDeletionLoop(db *db.Database, masterAddress string, httpAddress string) {

	for {

		keyfound, err := deleteKeys(db, masterAddress, httpAddress)

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

func deleteKeys(db *db.Database, masterAddress string, httpAddress string) (keyFound bool, err error) {
	var url string = "http://" + masterAddress + "/getDeletionHead"

	resp, err := http.Get(url)

	if err != nil {
		return false, err
	}

	var response KeyValuePair

	if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
		return false, err
	}

	if response.Err != nil {
		return false, err
	}

	if response.Key == "" {
		return false, nil
	}

	log.Printf("key is %v and value is %v", response.Key, response.Value)

	if err := db.DeleteReplicaKey(response.Key, []byte(response.Value)); err != nil {
		log.Printf("error deleteing key in replica %v", err)
		return false, err
	}
	// v, err := db.GetKey(response.Key)

	if err := deleteKeyFromDeletionQueue(string(response.Key), string(response.Value), masterAddress, httpAddress); err != nil {
		log.Printf("delete key from deletion queue failed: %v", err)
	}

	//change to true
	return false, nil

}

func deleteKeyFromDeletionQueue(key string, value string, masterAddress string, httpAddress string) (err error) {

	var url string = "http://" + masterAddress + "/deleteKeyFDQ?" + "key=" + key + "&value=" + value

	log.Printf("deleting key %v with value %v on server %v from deletion.go and my address is %v", key, value, masterAddress, httpAddress)

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
