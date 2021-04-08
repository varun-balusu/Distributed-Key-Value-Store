package election

import (
	"distribkv/usr/distributedkv/db"
	"distribkv/usr/distributedkv/hearbeat"
	"io"
	"io/ioutil"
	"log"
	"math/rand"
	"net/http"
	"strconv"
	"sync"
	"time"
)

var recievedHeartbeat bool

var ad string

type ElectionModule struct {
	tk    *time.Ticker
	state string
	db    *db.Database
}

var EC *ElectionModule

func ElectionLoop(peers []string, numNodes int, selfAddress string, db *db.Database) {
	//debug
	ad = selfAddress

	rand.Seed(time.Now().UnixNano())
	min := 150
	max := 300

	duration := time.Duration(rand.Intn(max-min+1)+min) * time.Millisecond

	tk := time.NewTicker(duration)

	EC = &ElectionModule{tk: tk, state: "FOLLOWER", db: db}

	var wg sync.WaitGroup

	numVotesRecieved := 1

	for range tk.C {
		EC.state = "CANDIDATE"
		//Trigger election happens here
		//
		//vote for self
		var url string = "http://" + selfAddress + "/triggerVoteForSelf"
		_, err := http.Get(url)
		if err != nil {
			TriggerTimeoutReset()
			log.Printf("there was an error voting for self: %v", err)
			continue
		}
		//finding out the current term
		url = "http://" + selfAddress + "/getCurrentTerm"
		resp, err := http.Get(url)
		if err != nil {
			TriggerTimeoutReset()
			log.Printf("error getting the current term %v", err)
			continue
		}
		//parse body for thr current term

		currentTerm, err := ParseBodyForTerm(resp.Body)
		if err != nil {
			TriggerTimeoutReset()
			log.Printf("error getting the current term %v", err)
			continue
		}

		resp.Body.Close()

		//request vote from all peers
		logLength := db.GetLogLength()
		for i := 0; i < len(peers); i++ {
			//send vote request
			var url string = "http://" + peers[i] + "/triggerVoteRequest?term=" + strconv.Itoa(currentTerm) + "&logLength=" + strconv.Itoa(logLength)

			wg.Add(1)
			go func(url string) {
				resp, err := http.Get(url)
				if err != nil {
					log.Printf("error fetching from url %v because of error %v", url, err)
					return
				}
				status, err := ioutil.ReadAll(resp.Body)
				if err != nil {
					log.Printf("recieved error parsing body: %v", err)
				}

				if string(status) == "ok" {
					numVotesRecieved++
				}

				wg.Done()
			}(url)
		}
		wg.Wait()
		//wait for replies from peers if on exit state has been set to follower then heartbeat was recieved
		//so abandon the election
		if EC.state == "FOLLOWER" {
			TriggerTimeoutReset()
			continue
		}

		//after election is over let all peers (including self) know that a new term has started
		//this gives them a vote for the next election
		clusterAddressArr := append(peers, selfAddress)
		for i := 0; i < len(clusterAddressArr); i++ {
			var url string = "http://" + clusterAddressArr[i] + "/triggerNextTerm?term=" + strconv.Itoa(currentTerm+1)
			http.Get(url)
		}

		// if number of votes recieved is the majority of the number of nodes in the cluster
		// then this node won the election, it becomes new leader and starts new goroutine to
		// send heartbeats to its followers
		if numVotesRecieved >= (numNodes/2)+1 {
			log.Printf("node at %v has won the election!!!", selfAddress)
			go hearbeat.SendHeartbeats(peers, true, selfAddress, EC.db)
			break
		}

		// Did not win the election return to follower state and wait for heartbeats
		TriggerTimeoutReset()

	}

	tk.Stop()

}

// Function the parse the body of the a response for the current term
func ParseBodyForTerm(r io.Reader) (currentTerm int, err error) {
	term, err := ioutil.ReadAll(r)
	if err != nil {
		// log.Printf("error parsing body for term %v", err)
		return -1, err
	}

	currentTerm, err = strconv.Atoi(string(term))
	if err != nil {
		return -1, err
	}

	return currentTerm, nil

}

// Upon recieving a heartbeat the election timeout is reset
// this function gets called everytime the web server recieves a heartbeat
// and resets the timeout to be a number between 150 - 300 milliseconds
func TriggerTimeoutReset() {
	EC.state = "FOLLOWER"
	// log.Printf("node %v becomes %v", ad, EC.state)
	// log.Println("reseting the timeout")
	rand.Seed(time.Now().UnixNano())
	min := 150
	max := 300

	duration := time.Duration(rand.Intn(max-min+1)+min) * time.Millisecond

	EC.tk.Reset(duration)
}
