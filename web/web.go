package web

import (
	"distribkv/usr/distributedkv/db"
	"distribkv/usr/distributedkv/replication"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/http"
	"strconv"
)

// Server contains a databse
type Server struct {
	db         *db.Database
	shardIndex int
	shardCount int
	addressMap map[int]string
	replicaArr []string
}

type ValueObject struct {
	Value string
}

// NewServer is the constructer for thr Server
func NewServer(db *db.Database, shardIndex int, shardCount int, addressMap map[int]string, replicaArr []string) (srv *Server) {
	if replicaArr != nil {
		for i := 0; i < len(replicaArr); i++ {
			log.Printf("current shardIndex is %v and shard replica is at %v", shardIndex, replicaArr[i])
		}
	}

	srv = &Server{db: db, shardIndex: shardIndex, shardCount: shardCount, addressMap: addressMap, replicaArr: replicaArr}

	return srv
}

// HandleGet is an exported function that handles get requests
func (s *Server) HandleGet(res http.ResponseWriter, req *http.Request) {
	req.ParseForm()
	// enc := json.NewEncoder(res)

	key := req.Form.Get("key")

	// var value string

	hash := fnv.New64()

	hash.Write([]byte(key))

	shardIdx := int(hash.Sum64() % uint64(s.shardCount))

	if s.shardIndex != shardIdx {
		s.redirectRequest(res, req, shardIdx)
	} else {
		v, _ := s.db.GetKey(key)

		value := string(v)
		//return just the value or the error encoded object
		fmt.Fprintf(res, value)
		// if err != nil {
		// 	fmt.Fprintf(res, string(v))
		// }

	}

	// enc.Encode(&ValueObject{
	// 	Value: value,
	// })

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

func (s *Server) HandleFetchLogIndex(res http.ResponseWriter, req *http.Request) {
	req.ParseForm()

	index, _ := strconv.Atoi(req.Form.Get("index"))

	// theLog := s.db.GetLog()

	c, err := s.db.GetLogAt(index)
	if err != nil {
		fmt.Fprintf(res, "Called GetLogAt and got error: %v\n", err)
	}

	// enc := json.NewEncoder(res)

	fmt.Fprintf(res, "Called GetLogAt and got command: %+v\n", c)

	// enc.Encode(theLog.Transcript[index])

}
func (s *Server) HandleGetLogLength(res http.ResponseWriter, req *http.Request) {
	length := s.db.GetLogLength()
	fmt.Fprintf(res, strconv.Itoa(length))
}
func (s *Server) HandleIncrementNextIndex(res http.ResponseWriter, req *http.Request) {
	req.ParseForm()

	address := req.Form.Get("address")
	err := s.db.IncrementNextIndex(address)
	if err != nil {
		fmt.Fprintf(res, "called incrementnextIndex and got error: %v", err)
	} else {
		fmt.Fprintf(res, "ok")
	}
}
func (s *Server) HandleGetNextLogEntry(res http.ResponseWriter, req *http.Request) {
	req.ParseForm()

	address := req.Form.Get("address")

	c, err := s.db.GetNextLogEntry(address)
	enc := json.NewEncoder(res)

	enc.Encode(&replication.LogEntry{
		Command: c,
		Err:     err,
	})

	fmt.Fprintf(res, "Called GetNExtLogEntry and got command %+v and error: %v\n", c, err)
}

func (s *Server) HandleDeleteExtraKeys(res http.ResponseWriter, req *http.Request) {

	err := s.db.DeleteExtraKeys(s.shardIndex, s.shardCount)

	fmt.Fprintf(res, "Called DeleteExtraKeys and got error: %v\n", err)
}

func (s *Server) FetchStatus(res http.ResponseWriter, req *http.Request) {
	fmt.Fprintf(res, "ok")
}

func (s *Server) HandleReadLog(res http.ResponseWriter, req *http.Request) {
	s.db.ReadLog()

}
