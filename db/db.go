package db

import (
	"bytes"
	"distribkv/usr/distributedkv/util"
	"errors"
	"log"
	"sync"

	bolt "go.etcd.io/bbolt"
)

var theDefaultBucket = []byte("Default_Bucket")
var replicationBucket = []byte("Replication_Queue")
var deletionBucket = []byte("Deletion_Queue")

var theLog Log

// Database is a open bold database
type Database struct {
	db        *bolt.DB
	readOnly  bool
	theLog    Log
	nextIndex int
	mu        sync.Mutex
	indexMap  map[string]int
}

type Command struct {
	Type  string
	Key   string
	Value string
}

type Log struct {
	Transcript []Command
}

// NewDatabase creates an instance of a boltdatabse with bolt.open() at the specified
// dbpath, return as error if bolt.open throws and error, and other wise if succesful
// NewDatabase return a pointer to a Database type, a function to close the database
// and no other errors
func NewDatabase(dbpath string, readOnly bool, replicaArr []string) (db *Database, closeDB func() error, err error) {

	boltDatabase, err := bolt.Open(dbpath, 0600, nil)

	if err != nil {
		return nil, nil, err

	}

	indexMap := make(map[string]int)

	for i := 0; i < len(replicaArr); i++ {
		indexMap[replicaArr[i]] = 0
	}

	db = &Database{db: boltDatabase, readOnly: readOnly, theLog: theLog, nextIndex: 0, indexMap: indexMap}
	closeDB = boltDatabase.Close

	createDefaultBucketError := db.CreateDefaultBucket()

	if createDefaultBucketError != nil {
		closeDB()
		return nil, nil, createDefaultBucketError
	}

	if !readOnly {
		createReplicationBucketError := db.CreateReplicationBucket()

		if createReplicationBucketError != nil {
			closeDB()
			return nil, nil, createReplicationBucketError
		}

		createDeletionBucketError := db.CreateDeletionBucket()

		if createDeletionBucketError != nil {
			closeDB()
			return nil, nil, createDeletionBucketError
		}
	}

	return db, closeDB, nil
}

func (d *Database) GetLog() Log {
	return d.theLog
}

// CreateDefaultBucket creates theDefaultBucket
func (d *Database) CreateDefaultBucket() error {
	err := d.db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(theDefaultBucket)
		if err != nil {
			return err
		}
		return nil
	})

	return err
}

func (d *Database) CreateReplicationBucket() error {
	err := d.db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(replicationBucket)
		if err != nil {
			return err
		}
		return nil
	})

	return err
}

func (d *Database) CreateDeletionBucket() error {
	err := d.db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(deletionBucket)
		if err != nil {
			return err
		}
		return nil
	})

	return err
}

// SetKey takes as input a key and value pair and starts a transaction
// it can return a rollback error if there is an error within the transaction
// it can also return an error if put fails from within the transaction
func (d *Database) SetKey(key string, value []byte) error {
	if d.readOnly {
		return errors.New("Database is in read-only mode set keys in Master instead")
	}

	current := &Command{
		Type:  "SET",
		Key:   key,
		Value: string(value),
	}

	d.theLog.Transcript = append(d.theLog.Transcript, *current)

	rollbackError := d.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(theDefaultBucket)
		putErr := b.Put([]byte(key), []byte(value))

		if putErr != nil {
			return putErr
		}

		r := tx.Bucket(replicationBucket)

		if err := r.Put([]byte(key), []byte(value)); err != nil {
			return err
		}

		return nil
	})
	return rollbackError

}

func (d *Database) SetReplicationKey(key string, value []byte) error {

	rollbackError := d.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(theDefaultBucket)
		putErr := b.Put([]byte(key), []byte(value))

		if putErr != nil {
			return putErr
		}

		return nil
	})
	return rollbackError

}

func (d *Database) SetKeyOnReplica(key string, value []byte) error {
	current := &Command{
		Type:  "SET",
		Key:   key,
		Value: string(value),
	}

	d.theLog.Transcript = append(d.theLog.Transcript, *current)

	rollbackError := d.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(theDefaultBucket)
		putErr := b.Put([]byte(key), []byte(value))

		if putErr != nil {
			return putErr
		}

		return nil
	})
	return rollbackError

}

// GetKey jdjioksjc
func (d *Database) GetKey(key string) (value []byte, err error) {
	log.Println(key)
	rollbackError := d.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(theDefaultBucket)
		v := b.Get([]byte(key))
		value = make([]byte, len(v))
		copy(value, v)
		return nil
	})

	return value, rollbackError
}

func (d *Database) ReplicationQueueHead() (key []byte, value []byte, err error) {
	err = d.db.View(func(tx *bolt.Tx) error {
		r := tx.Bucket(replicationBucket)
		k, v := r.Cursor().First()
		key = make([]byte, len(k))
		copy(key, k)
		value = make([]byte, len(v))
		copy(value, v)
		return nil
	})

	return key, value, err
}

func (d *Database) DeleteKeyFromReplicationQueue(key []byte, value []byte) (err error) {
	rollbackError := d.db.Update(func(tx *bolt.Tx) error {
		r := tx.Bucket(replicationBucket)
		v := r.Get(key)

		//key already deleted from replication queue and changes applied to replica
		if v == nil {
			return nil
		}

		if !bytes.Equal(value, v) {
			return errors.New("Cannot delete key from replication queue while changes are in flight")
		}

		if err := r.Delete(key); err != nil {
			return err
		}

		return nil
	})

	return rollbackError
}

// DeleteKey deletes a key from the current bucket
func (d *Database) DeleteReplicaKey(key string, value []byte) (err error) {
	rollbackError := d.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(theDefaultBucket)

		v := b.Get([]byte(key))

		log.Printf("inside db value of key is %v", string(v))

		//key was reset to another value while proccessing deletion queue so dont delete it
		if !bytes.Equal(v, value) {
			log.Printf("returning out of func because bytes are not equal")

			// return nil
			return errors.New("bytes do not match in delete")
		}

		if v == nil {
			log.Printf("returning frm fuction due to nil value")
			return nil
		}

		deleteError := b.Delete([]byte(key))

		if deleteError != nil {
			log.Printf("getting error is deletereplica %v", deleteError)
			return deleteError
		}
		v = b.Get([]byte(key))
		log.Printf("new value for key after delete is %v", v)
		return nil
	})

	return rollbackError
}

//bih yah
func (d *Database) DeleteKeyFromDeletionQueue(key []byte, value []byte) (err error) {
	rollbackError := d.db.Update(func(tx *bolt.Tx) error {
		r := tx.Bucket(deletionBucket)
		v := r.Get(key)

		//key already deleted from replication queue and changes applied to replica
		if v == nil {
			return nil
		}

		if !bytes.Equal(value, v) {
			return errors.New("Cannot delete key from deletion queue while changes are in flight")
		}

		if err := r.Delete(key); err != nil {
			return err
		}

		return nil
	})

	return rollbackError
}

func (d *Database) DeletionQueueHead() (key []byte, value []byte, err error) {
	err = d.db.View(func(tx *bolt.Tx) error {
		d := tx.Bucket(deletionBucket)
		k, v := d.Cursor().First()
		key = make([]byte, len(k))
		copy(key, k)
		value = make([]byte, len(v))
		copy(value, v)
		return nil
	})
	// log.Printf("Deletion queue head is %v", key)
	return key, value, err
}

func (d *Database) DeleteKey(key string) (err error) {
	if d.readOnly {
		return errors.New("Cannot delete from read-only database delete from Master instead")
	}
	current := &Command{
		Type: "DELETE",
		Key:  key,
	}

	d.theLog.Transcript = append(d.theLog.Transcript, *current)

	rollbackError := d.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(theDefaultBucket)
		value := b.Get([]byte(key))
		deleteError := b.Delete([]byte(key))

		if deleteError != nil {
			return deleteError
		}

		d := tx.Bucket(deletionBucket)
		if err := d.Put([]byte(key), []byte(value)); err != nil {
			return err
		}

		return nil
	})

	return rollbackError
}

func (d *Database) DeleteKeyOnReplica(key string) (err error) {
	current := &Command{
		Type: "DELETE",
		Key:  key,
	}

	d.theLog.Transcript = append(d.theLog.Transcript, *current)

	rollbackError := d.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(theDefaultBucket)
		deleteError := b.Delete([]byte(key))

		if deleteError != nil {
			return deleteError
		}

		return nil
	})

	return rollbackError
}

func (d *Database) ReadLog() {

	for i := 0; i < len(d.theLog.Transcript); i++ {
		log.Printf("command is %+v", d.theLog.Transcript[i])
	}
}

func (d *Database) GetLogLength() int {

	return len(d.theLog.Transcript)
}

func (d *Database) IncrementNextIndex(address string) error {
	index, present := d.indexMap[address]
	if !present {
		return errors.New("address not found in index Map")
	}
	d.mu.Lock()
	index++
	d.indexMap[address] = index
	d.mu.Unlock()

	return nil
}

func (d *Database) GetNextLogEntry(address string) (c Command, err error) {
	if d.readOnly {
		return Command{}, errors.New("Cant read from slave Log")
	}
	log := d.theLog.Transcript
	index := d.indexMap[address]
	if index >= len(log) {
		return Command{}, errors.New("No next Entry avaliable")
	}

	return log[index], nil

}

func (d *Database) GetLogAt(index int) (c Command, err error) {
	log.Println(index)
	log.Println(d.theLog.Transcript)
	if index > len(d.theLog.Transcript)-1 {
		return Command{}, errors.New("invalid Index requested")
	}
	log.Printf("the command in db is %+v", d.theLog.Transcript[index])
	return d.theLog.Transcript[index], nil

}

func (d *Database) DeleteExtraKeys(currentShardIndex int, shardCount int) (err error) {

	var keys []string

	rollbackError := d.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(theDefaultBucket)

		return b.ForEach(func(k []byte, v []byte) error {
			if util.IsExtraKey(string(k), currentShardIndex, shardCount) {
				keys = append(keys, string(k))
			}
			return nil
		})
	})

	if rollbackError != nil {
		return rollbackError
	}

	updateError := d.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(theDefaultBucket)
		log.Println(len(keys))
		for _, key := range keys {
			log.Println(key)
			err := b.Delete([]byte(key))

			if err != nil {
				return err
			}
		}
		return nil
	})

	if updateError != nil {
		return updateError
	} else {
		return nil
	}

}

func (d *Database) ExecuteCommand(command Command) (err error) {
	if command.Type == "SET" {
		return d.SetKeyOnReplica(string(command.Key), []byte(command.Value))
	} else {
		return d.DeleteKeyOnReplica(string(command.Key))
	}
}
