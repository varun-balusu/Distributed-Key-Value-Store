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

var theLog Log

// Database is a open bold database
type Database struct {
	db       *bolt.DB
	ReadOnly bool
	TheLog   Log
	mu       sync.Mutex
	IndexMap map[string]int
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

	db = &Database{db: boltDatabase, ReadOnly: readOnly, TheLog: theLog, IndexMap: indexMap}
	closeDB = boltDatabase.Close

	createDefaultBucketError := db.CreateDefaultBucket()

	if createDefaultBucketError != nil {
		closeDB()
		return nil, nil, createDefaultBucketError
	}

	return db, closeDB, nil
}

func (d *Database) GetLog() Log {
	return d.TheLog
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

// SetKey takes as input a key and value pair and starts a transaction
// it can return a rollback error if there is an error within the transaction
// it can also return an error if put fails from within the transaction
func (d *Database) SetKey(key string, value []byte) error {
	if d.ReadOnly {
		return errors.New("Database is in read-only mode set keys in Master instead")
	}

	current := &Command{
		Type:  "SET",
		Key:   key,
		Value: string(value),
	}

	d.TheLog.Transcript = append(d.TheLog.Transcript, *current)

	// rollbackError := d.db.Update(func(tx *bolt.Tx) error {
	// 	b := tx.Bucket(theDefaultBucket)
	// 	putErr := b.Put([]byte(key), []byte(value))

	// 	if putErr != nil {
	// 		return putErr
	// 	}

	// 	return nil
	// })
	// return rollbackError

	return nil

}

func (d *Database) ExecuteSetCommand(c Command) error {
	key := c.Key
	value := c.Value

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

	d.TheLog.Transcript = append(d.TheLog.Transcript, *current)

	// rollbackError := d.db.Update(func(tx *bolt.Tx) error {
	// 	b := tx.Bucket(theDefaultBucket)
	// 	putErr := b.Put([]byte(key), []byte(value))

	// 	if putErr != nil {
	// 		return putErr
	// 	}

	// 	return nil
	// })
	return nil

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

func (d *Database) DeleteKey(key string) (err error) {
	if d.ReadOnly {
		return errors.New("Cannot delete from read-only database delete from Master instead")
	}
	current := &Command{
		Type: "DELETE",
		Key:  key,
	}

	d.TheLog.Transcript = append(d.TheLog.Transcript, *current)

	// rollbackError := d.db.Update(func(tx *bolt.Tx) error {
	// 	b := tx.Bucket(theDefaultBucket)
	// 	deleteError := b.Delete([]byte(key))

	// 	if deleteError != nil {
	// 		return deleteError
	// 	}

	// 	return nil
	// })

	return nil
}

func (d *Database) ExecuteDeleteCommand(c Command) (err error) {
	key := c.Key

	return d.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(theDefaultBucket)
		deleteError := b.Delete([]byte(key))

		if deleteError != nil {
			return deleteError
		}

		return nil
	})

}

func (d *Database) DeleteKeyOnReplica(key string) (err error) {
	current := &Command{
		Type: "DELETE",
		Key:  key,
	}

	d.TheLog.Transcript = append(d.TheLog.Transcript, *current)

	// rollbackError := d.db.Update(func(tx *bolt.Tx) error {
	// 	b := tx.Bucket(theDefaultBucket)
	// 	deleteError := b.Delete([]byte(key))

	// 	if deleteError != nil {
	// 		return deleteError
	// 	}

	// 	return nil
	// })

	return nil
}

func (d *Database) ReadLog() {

	for i := 0; i < len(d.TheLog.Transcript); i++ {
		log.Printf("command is %+v", d.TheLog.Transcript[i])
	}
}

func (d *Database) GetLogLength() int {

	return len(d.TheLog.Transcript)
}

func (d *Database) IncrementNextIndex(address string) error {
	// d.mu.Lock()
	d.mu.Lock()
	index, present := d.IndexMap[address]
	if !present {
		return errors.New("address not found in index Map")

	}

	index++
	d.IndexMap[address] = index
	// d.mu.Unlock()
	defer d.mu.Unlock()
	return nil
}

func (d *Database) GetNextLogEntry(address string) (c Command, err error) {
	if d.ReadOnly {
		return Command{}, errors.New("Cant read from slave Log")
	}

	alog := d.TheLog.Transcript
	d.mu.Lock()
	index := d.IndexMap[address]
	d.mu.Unlock()
	if index >= len(alog) {
		return Command{}, errors.New("No next Entry avaliable")
	}

	log.Printf("the next command for address %v is %+v", address, alog[index])
	return alog[index], nil

}

func (d *Database) GetLogAt(index int) (c Command, err error) {
	log.Println(index)
	log.Println(d.TheLog.Transcript)
	if index > len(d.TheLog.Transcript)-1 {
		return Command{}, errors.New("invalid Index requested")
	}
	log.Printf("the command in db is %+v", d.TheLog.Transcript[index])
	return d.TheLog.Transcript[index], nil

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
