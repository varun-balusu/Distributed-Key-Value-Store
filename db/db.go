package db

import (
	bolt "go.etcd.io/bbolt"
)

var theDefaultBucket = []byte("MyBucket")

// Database is a open bold database
type Database struct {
	db *bolt.DB
}

// NewDatabase creates an instance of a boltdatabse with bolt.open() at the specified
// dbpath, return as error if bolt.open throws and error, and other wise if succesful
// NewDatabase return a pointer to a Database type, a function to close the database
// and no other errors
func NewDatabase(dbpath string) (db *Database, closeDB func() error, err error) {

	boltDatabase, err := bolt.Open(dbpath, 0600, nil)

	if err != nil {
		return nil, nil, err

	}

	db = &Database{db: boltDatabase}
	closeDB = boltDatabase.Close

	createDefaultBucketError := db.CreateDefaultBucket()

	if createDefaultBucketError != nil {
		closeDB()
		return nil, nil, createDefaultBucketError
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

// SetKey takes as input a key and value pair and starts a transaction
// it can return a rollback error if there is an error within the transaction
// it can also return an error if put fails from within the transaction
func (d *Database) SetKey(key string, value []byte) error {
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
		value = v
		return nil
	})

	return value, rollbackError
}

// DeleteKey deletes a key from the current bucket
func (d *Database) DeleteKey(key string) (err error) {

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
