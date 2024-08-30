package configs

import (
	"fmt"
	"os"
)

type KafkaConfigType struct {
	BootstrapServers string
	GroupId          string
	Topic            string
}

var KafkaConfig KafkaConfigType

func SetKafkaConfig() {
	hostServer := os.Getenv("BOOTSTRAP_SERVER_HOST")
	hostPort := os.Getenv("BOOTSTRAP_SERVER_PORT")

	KafkaConfig.BootstrapServers = fmt.Sprintf("%s:%s", hostServer, hostPort)
	KafkaConfig.GroupId = os.Getenv("GROUP_ID")
	KafkaConfig.Topic = os.Getenv("TOPIC_NAME")
}
