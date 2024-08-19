package lib

import (
	"encoding/json"
	"log"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/donghquinn/kafka-cdc/configs"
)

type GateData struct {
	Payload GateNodeSchema `json:"payload"`
}

type GateNodeSchema struct {
	Block_health_id int `json:"block_health_id"`
	Network_id int	`json:"network_id"`
	Prior_blocknumber int	`json:"prior_blocknumber"`
	Current_blocknumber int	`json:"current_blocknumber"`
	Block_diff int	`json:"block_diff"`
	Health_status string	`json:"health_status"`
	Reg_date int	`json:"reg_date"`
	Mod_date int	`json:"mod_date"`
}

func KafkaConsumer(config configs.KafkaConfigType) *kafka.Consumer {
	// log.Printf("Bootstrap Server: %s", config.BootstrapServers)

    c, err := kafka.NewConsumer(&kafka.ConfigMap{
        "bootstrap.servers": config.BootstrapServers,
        "group.id":          config.GroupId,
        // "auto.offset.reset": "earliest",
    })

    if err != nil {
        log.Printf("Connect Broker Error: %v", err)
		return nil
    }

    return c
}

func Consumer()  {
	kafkaConfig := configs.KafkaConfig

    c := KafkaConsumer(kafkaConfig)

	log.Printf("Start Subscribe Topic: %s", kafkaConfig.Topic)

    c.SubscribeTopics([]string{kafkaConfig.Topic}, nil)

    defer c.Close()

    for {
        msg, err := c.ReadMessage(-1)

		var nodeInfo GateData

        if err == nil {
			marshErr := json.Unmarshal(msg.Value, &nodeInfo)

			if marshErr == nil {
				log.Printf("node_id: %d, prior: %d current: %d, block_diff: %d, health_status: %s", nodeInfo.Payload.Network_id, nodeInfo.Payload.Prior_blocknumber, nodeInfo.Payload.Current_blocknumber, nodeInfo.Payload.Block_diff, nodeInfo.Payload.Health_status)
			} else {
				log.Printf("Marshall Error: %v", marshErr)
			}
        } else {
            // The client will automatically try to recover from all errors.
            log.Printf("Consumer error: %v (%v)\n", err, msg)
        }
    }
}
