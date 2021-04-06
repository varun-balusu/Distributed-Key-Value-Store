package hearbeat

import (
	"io/ioutil"
	"log"
	"net/http"
	"time"
)

func SendHeartbeats(replicaArr []string) {

	duration := time.Duration(2) * time.Second

	tk := time.NewTicker(duration)

	for range tk.C {

		for i := 0; i < len(replicaArr); i++ {
			TriggerHeartbeat(replicaArr[i])
		}
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
