package commiter

import (
	"distribkv/usr/distributedkv/db"
	"io/ioutil"
	"log"
	"net/http"
	"strconv"
	"time"
)

type CommiterModule struct {
	commitIndex int
}

func ApplyLatestChangesLoop(dba *db.Database, masterAddress string, myAddress string) {
	CM := &CommiterModule{commitIndex: -1}
	for {
		var url string = "http://" + myAddress + "/getCurrentClusterLeader"
		resp, err := http.Get(url)
		if err != nil {
			time.Sleep(time.Second * 2)
			continue
		}
		currentLeaderAddress, _ := ioutil.ReadAll(resp.Body)
		resp.Body.Close()
		if string(currentLeaderAddress) == myAddress {
			log.Printf("ending commiter on server %v", myAddress)
			break
		} else {
			masterAddress = string(currentLeaderAddress)
		}

		err = FindAndApplyLatestCommits(dba, masterAddress, myAddress, CM)

		if err != nil {
			time.Sleep(time.Second * 2)
		}

	}
}

func FindAndApplyLatestCommits(dba *db.Database, masterAddress string, myAddress string, CM *CommiterModule) error {
	theLog := dba.TheLog
	latestIndex := strconv.Itoa(len(theLog.Transcript))
	url := "http://" + masterAddress + "/confirmEntry?latestIndex=" + latestIndex

	resp, err := http.Get(url)
	if err != nil {
		log.Printf("error confirming entry: %v", err)
		return err
	}
	leaderCommitIndex, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Printf("error reading body for leader commit index: %v", err)
		return err
	}
	resp.Body.Close()
	// ignored error
	leaderCommitIndexNum, _ := strconv.Atoi(string(leaderCommitIndex))
	// log.Printf("my address is %v and my leadersCommitIndex is %d", myAddress, leaderCommitIndexNum)
	if leaderCommitIndexNum > CM.commitIndex {
		// log.Printf("my address is %v and im commting from %dto %d", myAddress, CM.commitIndex, leaderCommitIndexNum)
		temp := CM.commitIndex
		CM.commitIndex = leaderCommitIndexNum
		go func(startIndex int, endIndex int) {
			if startIndex == -1 {
				startIndex = 0
				CM.commitIndex = 0
			}
			for i := startIndex; i <= endIndex; i++ {
				theLog := dba.TheLog
				if i < len(theLog.Transcript) {
					c := theLog.Transcript[i]
					dba.ExecuteSetCommand(c)
				} else {
					break
				}
			}
		}(temp, leaderCommitIndexNum)
	}
	return nil
}
