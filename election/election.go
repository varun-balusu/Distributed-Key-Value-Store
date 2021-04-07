package election

import (
	"encoding/json"
	"log"
	"math/rand"
	"net/http"
	"strconv"
	"time"
)

var recievedHeartbeat bool

type ElectionCounter struct {
	tk          *time.Ticker
	currentTerm int
	state       string
}

type VoteReply struct {
	Term int
	Err  error
}

var EC *ElectionCounter

func ElectionLoop(peers []string, numNodes int, selfAddress string) {

	rand.Seed(time.Now().UnixNano())
	min := 150
	max := 300

	duration := time.Duration(rand.Intn(max-min+1)+min) * time.Millisecond

	log.Printf("duration in %v is %v", selfAddress, duration)
	// duration = time.Duration(150) * time.Millisecond

	tk := time.NewTicker(duration)

	EC = &ElectionCounter{tk: tk, state: "FOLLOWER"}

	// var wg sync.WaitGroup

	// numVotesRecieved := 1

	for range tk.C {
		EC.currentTerm++
		EC.state = "CANDIDATE"
		http.Get("http://" + selfAddress + "/triggerNextTerm?term=" + strconv.Itoa(EC.currentTerm))

		for i := 0; i < len(peers); i++ {
			var url string = "http://" + peers[i] + "/triggerVoteRequest?term=" + strconv.Itoa(EC.currentTerm)
			go func(url string) {
				resp, _ := http.Get(url)
				var voteReply VoteReply
				json.NewDecoder(resp.Body).Decode(&voteReply)

				if voteReply.Term > EC.currentTerm {
					log.Printf("term is out of date returning to follower")
					EC.state = "FOLLOWER"
					return
				}

				if voteReply.Term <= EC.currentTerm {

				}

			}(url)
		}

		if EC.state == "FOLLOWER" {
			TriggerTimeoutReset()
			continue
		}

	}

}

func TriggerTimeoutReset() {
	EC.state = "FOLLOWER"
	log.Println("reseting the timeout")

	rand.Seed(time.Now().UnixNano())
	min := 150
	max := 300

	duration := time.Duration(rand.Intn(max-min+1)+min) * time.Millisecond

	EC.tk.Reset(duration)
}
