package election

import (
	"distribkv/usr/distributedkv/hearbeat"
	"io/ioutil"
	"log"
	"math/rand"
	"net/http"
	"strconv"
	"sync"
	"time"
)

var recievedHeartbeat bool

type ElectionCounter struct {
	tk *time.Ticker
}

var EC *ElectionCounter

func ElectionLoop(peers []string, numNodes int, selfAddress string) {

	rand.Seed(time.Now().UnixNano())
	min := 5
	max := 5

	duration := time.Duration(rand.Intn(max-min+1)+min) * time.Second

	tk := time.NewTicker(duration)

	EC = &ElectionCounter{tk: tk}

	var wg sync.WaitGroup

	numVotesRecieved := 1

	for i := 0; i < len(peers); i++ {
		log.Printf("replica with address: %v has peers:", selfAddress)
		log.Printf("%v\n", peers[i])
	}

	for range tk.C {
		log.Println("timeout occured inside the for loop")
		//Trigger election happens here
		log.Println("votting for self")
		//vote for self
		var url string = "http://" + selfAddress + "/triggerVoteForSelf"
		_, err := http.Get(url)
		if err != nil {
			log.Printf("there was an error voting for self: %v", err)
		}
		//finding out the current term
		url = "http://" + selfAddress + "/getCurrentTerm"
		resp, err := http.Get(url)
		if err != nil {
			log.Printf("error getting the current term %v", err)
		}
		//parse body for thr current term

		term, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			log.Printf("error parsing body for term %v", err)
		}

		currentTerm, _ := strconv.Atoi(string(term))

		resp.Body.Close()

		for i := 0; i < len(peers); i++ {
			//send vote request
			var url string = "http://" + peers[i] + "/triggerVoteRequest?term=" + string(term)
			log.Printf("triggering vote request from %v to url: %v", selfAddress, url)

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
		log.Printf("num votes recieved is %d", numVotesRecieved)
		clusterAddressArr := append(peers, selfAddress)
		for i := 0; i < len(clusterAddressArr); i++ {
			var url string = "http://" + clusterAddressArr[i] + "/triggerNextTerm?term=" + strconv.Itoa(currentTerm+1)
			http.Get(url)
		}

		if numVotesRecieved == 2 {
			log.Printf("replica at %v won the election", selfAddress)

			go hearbeat.SendHeartbeats(peers)
			break
		}

		TriggerTimeoutReset()

	}

	tk.Stop()

}

func TriggerTimeoutReset() {
	log.Println("reseting the timeout")
	rand.Seed(time.Now().UnixNano())
	min := 5
	max := 10

	duration := time.Duration(rand.Intn(max-min+1)+min) * time.Second

	EC.tk.Reset(duration)
}

// func ReturnVotes(peers []string) {
// 	for i := 0; i < len(peers); i++ {
// 		var url string = "http://" + peers[i] + "/returnVote"
// 		http.Get()
// 	}
// }