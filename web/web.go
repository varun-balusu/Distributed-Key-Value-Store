package web

import (
	"distribkv/usr/distributedkv/db"
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

	value, err := s.db.GetKey(key)

	fmt.Fprintf(res, "Called get and got value: %q and error: %v\n", value, err)

}

// HandleSet is an exported function that handles set requests
func (s *Server) HandleSet(res http.ResponseWriter, req *http.Request) {
	req.ParseForm()

	key := req.Form.Get("key")
	value := req.Form.Get("value")

	hash := fnv.New64()

	hash.Write([]byte(key))

	shardIdx := int(hash.Sum64() % uint64(s.shardCount))

	if shardIdx != s.shardIndex {
		var redirectAddress string = s.addressMap[shardIdx]

		url := "http://" + redirectAddress + req.RequestURI

		fmt.Fprintf(res, "redirecting to %v", url)

		response, err := http.Get(url)

		if err != nil {
			res.WriteHeader(500)
			fmt.Printf("Error rediricting the request: %v", err)
			return
		}

		defer response.Body.Close()

		io.Copy(res, response.Body)

	}

	err := s.db.SetKey(key, []byte(value))

	fmt.Fprintf(res, "Called set got error:%v\n", err)

	return

}

// HandleDelete deletes a key
func (s *Server) HandleDelete(res http.ResponseWriter, req *http.Request) {
	req.ParseForm()

	key := req.Form.Get("key")

	err := s.db.DeleteKey(key)

	fmt.Fprintf(res, "Called Delete and got error: %v\n", err)

}
