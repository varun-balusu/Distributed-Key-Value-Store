package hearbeat

import (
	"io/ioutil"
	"log"
	"net/http"
	"time"
)

func SendHeartbeats(replicaArr []string) {

	duration := time.Duration(50) * time.Millisecond

	tk := time.NewTicker(duration)
	x := 0
	for range tk.C {
		if x == 10 {
			break
		}
		for i := 0; i < len(replicaArr); i++ {
			TriggerHeartbeat(replicaArr[i])
		}

		x++
	}

	defer tk.Stop()

}

func TriggerHeartbeat(address string) {
	var url string = "http://" + address + "/triggerHeartbeat"

	resp, err := http.Get(url)
	if err != nil {
		log.Printf("error sending heartbeats: %v", err)
	}

	status, err := ioutil.ReadAll(resp.Body)

	if err != nil {
		log.Printf("error parsing body: %v", err)
	} else {
		log.Printf("got ack from replica with status: %v", string(status))
	}

	defer resp.Body.Close()
}
