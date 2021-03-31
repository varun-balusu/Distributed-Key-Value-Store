package db

import (
	"bytes"
	"distribkv/usr/distributedkv/util"
	"errors"
	"log"

	bolt "go.etcd.io/bbolt"
)

var theDefaultBucket = []byte("Default_Bucket")
var replicationBucket = []byte("Replication_Queue")
var deletionBucket = []byte("Deletion_Queue")

// Database is a open bold database
type Database struct {
	db       *bolt.DB
	readOnly bool
}

// NewDatabase creates an instance of a boltdatabse with bolt.open() at the specified
// dbpath, return as error if bolt.open throws and error, and other wise if succesful
// NewDatabase return a pointer to a Database type, a function to close the database
// and no other errors
func NewDatabase(dbpath string, readOnly bool) (db *Database, closeDB func() error, err error) {

	boltDatabase, err := bolt.Open(dbpath, 0600, nil)

	if err != nil {
		return nil, nil, err

	}

	db = &Database{db: boltDatabase, readOnly: readOnly}
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

// GetKey jdjioksjc
func (d *Database) GetKey(key string) (value []byte, err error) {

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
func (d *Database) DeleteReplicaKey(key string) (err error) {
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

//bih yah
func (d *Database) DeleteKeyFromDeletionQueue(key []byte, value []byte) (err error) {
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
	log.Println(key)
	return key, value, err
}

func (d *Database) DeleteKey(key string) (err error) {
	if d.readOnly {
		return errors.New("Cannot delete from read-only database delete from Master instead")
	}
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
