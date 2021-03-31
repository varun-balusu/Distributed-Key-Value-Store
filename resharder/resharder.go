package resharder

import (
	"distribkv/usr/distributedkv/config"
	"io"
	"log"
	"os"
	"strconv"

	"github.com/BurntSushi/toml"
)

//0 shards case
//return errors instead of log.fatal
//only want log.fatal in the main file
func DoubleShards(configPath string, numReplicas int) (err error) {

	var c config.Config

	if _, err := toml.DecodeFile(configPath, &c); err != nil {
		log.Fatalf("toml.DecodeFile(%q): %v", configPath, err)
	}

	newShardCount := len(c.Shards) * 2

	currentDirectory, err := os.Getwd()
	if err != nil {
		log.Fatalf("failed to find current directory with error %v", err)
	}

	file, err := os.OpenFile(configPath, os.O_APPEND|os.O_RDWR, 0644)
	if err != nil {
		//failed to open config file
		log.Fatal(err)
	}

	size := len(c.Shards)
	copyIndex := 0

	for size < newShardCount {
		var shard config.Shard

		var baseHost int = 1
		var port int = 8080 + size
		shard.Address = "127.0.0." + strconv.Itoa(baseHost) + ":" + strconv.Itoa(port)
		shard.Index = size
		shard.Name = "server-" + strconv.Itoa(size)
		shard.Replicas = createReplicaArray(baseHost+1, numReplicas, port)

		updateConfig(file, shard)

		dstFilePath := currentDirectory + "/" + shard.Name + ".db"
		srcFilePath := currentDirectory + "/" + c.Shards[copyIndex].Name + ".db"

		copyDB(srcFilePath, dstFilePath)

		size++
		copyIndex++

	}

	if err := file.Close(); err != nil {
		// failed to close the file
		log.Fatal(err)

	}

	log.Printf("new shard count is %d", newShardCount)

	return nil

}

func createReplicaArray(baseHost int, numReplicas int, port int) (replicaArr []string) {

	for i := 1; i < numReplicas+1; i++ {
		replicaArr = append(replicaArr, "127.0.0."+strconv.Itoa(baseHost)+":"+strconv.Itoa(port))
		baseHost++
	}
	log.Println(len(replicaArr))
	return replicaArr

}

func copyDB(srcFilePath string, dstFilePath string) {

	destination, err := os.Create(dstFilePath)
	if err != nil {
		log.Fatalf("failed to create destination file with error: %v", err)
	}

	source, err := os.Open(srcFilePath)

	if _, err := io.Copy(destination, source); err != nil {
		log.Fatalf("failed to copy files with error : %v", err)
	} else {
		log.Printf("copying from %v to %v", srcFilePath, dstFilePath)
	}

	if err := destination.Close(); err != nil {
		log.Fatalf("failed to close destination file with error: %v", err)
	}

	if err := source.Close(); err != nil {
		log.Fatalf("failed to close source file with error: %v", err)
	}

}

func updateConfig(file *os.File, shard config.Shard) {

	if _, err := file.WriteString("\n[[shards]]\n"); err != nil {
		//failed to write to file
		log.Fatal(err)
	}

	if err := toml.NewEncoder(file).Encode(shard); err != nil {
		// failed to encode
		log.Fatal(err)

	}

}
