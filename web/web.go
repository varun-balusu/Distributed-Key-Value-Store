package web

import (
	"distribkv/usr/distributedkv/db"
	"distribkv/usr/distributedkv/replication"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"net/http"
)

// Server contains a databse
type Server struct {
	db         *db.Database
	shardIndex int
	shardCount int
	addressMap map[int]string
}

// NewServer is the constructer for thr Server
func NewServer(db *db.Database, shardIndex int, shardCount int, addressMap map[int]string) (srv *Server) {

	srv = &Server{db: db, shardIndex: shardIndex, shardCount: shardCount, addressMap: addressMap}

	return srv
}

// HandleGet is an exported function that handles get requests
func (s *Server) HandleGet(res http.ResponseWriter, req *http.Request) {
	req.ParseForm()

	key := req.Form.Get("key")

	hash := fnv.New64()

	hash.Write([]byte(key))

	shardIdx := int(hash.Sum64() % uint64(s.shardCount))

	if s.shardIndex != shardIdx {
		s.redirectRequest(res, req, shardIdx)
	} else {
		value, err := s.db.GetKey(key)
		fmt.Fprintf(res, "Called get and got value: %q and error: %v\n", value, err)
	}

}

func (s *Server) redirectRequest(res http.ResponseWriter, req *http.Request, shardIdx int) {

	var redirectAddress string = s.addressMap[shardIdx]

	url := string("http://" + redirectAddress + req.RequestURI)

	fmt.Fprintf(res, "redirecting to %v\n", url)

	response, err := http.Get(url)

	if err != nil {
		res.WriteHeader(500)
		fmt.Printf("Error rediricting the request: %v", err)
		return
	}

	defer response.Body.Close()

	io.Copy(res, response.Body)

}

// HandleSet is an exported function that handles set requests
func (s *Server) HandleSet(res http.ResponseWriter, req *http.Request) {
	req.ParseForm()

	key := req.FormValue("key")
	value := req.Form.Get("value")

	hash := fnv.New64()

	hash.Write([]byte(key))

	fmt.Fprintf(res, "hash of key is %d", hash.Sum64())

	shardIdx := int(hash.Sum64() % uint64(s.shardCount))

	if shardIdx != s.shardIndex {

		s.redirectRequest(res, req, shardIdx)

	} else {
		err := s.db.SetKey(key, []byte(value))
		fmt.Fprintf(res, "Called set for key %v got error:%v\n", key, err)

	}

	return

}

// HandleDelete deletes a key
func (s *Server) HandleDelete(res http.ResponseWriter, req *http.Request) {
	req.ParseForm()

	key := req.Form.Get("key")

	hash := fnv.New64()

	hash.Write([]byte(key))

	shardIdx := int(hash.Sum64() % uint64(s.shardCount))

	if shardIdx != s.shardIndex {
		s.redirectRequest(res, req, shardIdx)
	} else {
		err := s.db.DeleteKey(key)

		fmt.Fprintf(res, "Called Delete and got error: %v\n", err)
	}

}

func (s *Server) HandleDeleteExtraKeys(res http.ResponseWriter, req *http.Request) {

	err := s.db.DeleteExtraKeys(s.shardIndex, s.shardCount)

	fmt.Fprintf(res, "Called DeleteExtraKeys and got error: %v\n", err)
}

func (s *Server) HandleReplicationQueueHead(res http.ResponseWriter, req *http.Request) {

	enc := json.NewEncoder(res)

	key, value, err := s.db.ReplicationQueueHead()

	enc.Encode(&replication.KeyValuePair{
		Key:   string(key),
		Value: string(value),
		Err:   err,
	})

	fmt.Fprintf(res, "Called ReplicationQueueHead and got key %v and value %v and error: %v\n", string(key), string(value), err)
}

func (s *Server) HandleDeletionQueueHead(res http.ResponseWriter, req *http.Request) {

	enc := json.NewEncoder(res)

	key, value, err := s.db.DeletionQueueHead()

	enc.Encode(&replication.KeyValuePair{
		Key:   string(key),
		Value: string(value),
		Err:   err,
	})

	fmt.Fprintf(res, "Called DeletionQueueHead and got key %v and value %v and error: %v\n", string(key), string(value), err)
}

func (s *Server) HandleDeleteKeyFromReplicationQueue(res http.ResponseWriter, req *http.Request) {
	req.ParseForm()
	key := req.FormValue("key")
	value := req.Form.Get("value")

	err := s.db.DeleteKeyFromReplicationQueue([]byte(key), []byte(value))

	if err != nil {
		res.WriteHeader(500)
		fmt.Fprintf(res, "error: %v", err)
		return
	}

	fmt.Fprintf(res, "ok")
}

func (s *Server) HandleDeleteKeyFromDeletionQueue(res http.ResponseWriter, req *http.Request) {
	req.ParseForm()
	key := req.FormValue("key")
	value := req.Form.Get("value")

	err := s.db.DeleteKeyFromDeletionQueue([]byte(key), []byte(value))

	if err != nil {
		res.WriteHeader(500)
		fmt.Fprintf(res, "error: %v", err)
		return
	}

	fmt.Fprintf(res, "ok")
}
