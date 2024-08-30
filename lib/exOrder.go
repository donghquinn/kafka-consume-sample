package lib

import (
	"encoding/json"
	"log"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/donghquinn/kafka-cdc/configs"
	"github.com/donghquinn/kafka-cdc/util"
)

type TradeData struct {
	Payload ExOrderResult `json:"payload"`
}

type ExOrderResult struct {
	Uid      int    `json:"uid"`
	MemUid   int    `json:"memUid"`
	MemId    string `json:"memId"`
	BsType   string `json:"bsType"`
	AstCode  string `json:"astCode"`
	ResVol   string `json:"resVol"`
	ResSum   string `json:"resSum"`
	ExOrdMst string `json:"exOrdMst"`
}

func kafkaConsumer(config configs.KafkaConfigType) *kafka.Consumer {
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

func ConsumeExOrder() {
	kafkaConfig := configs.KafkaConfig
	client := kafkaConsumer(kafkaConfig)

	log.Printf("Start Subscribe Topic: %s", kafkaConfig.Topic)

	client.SubscribeTopics([]string{kafkaConfig.Topic}, nil)

	defer client.Close()

	for {
		msg, err := client.ReadMessage(-1)

		var exInfo TradeData

		if err == nil {
			marshErr := json.Unmarshal(msg.Value, &exInfo)

			if marshErr == nil {
				resVol, resErr := util.ConvertStringToIntWithDecoding(exInfo.Payload.ResVol, 8)

				if resErr != nil {
					log.Printf("Convert resVol to Int Error: %v", resErr)
				}

				resSum, resSumErr := util.ConvertStringToIntWithDecoding(exInfo.Payload.ResSum, 8)

				if resSumErr != nil {
					log.Printf("Convert resSum to Int Error: %v", resSumErr)
				}

				log.Printf("ExOrder Detected: Uid: %d, MemId: %s, BsType: %s, AstCode: %s, ResVol: %s, ResSum: %s, exOrdMst: %s", exInfo.Payload.Uid, exInfo.Payload.MemId, exInfo.Payload.BsType, exInfo.Payload.AstCode, resVol.FloatString(8), resSum.FloatString(8), exInfo.Payload.ExOrdMst)
			} else {
				log.Printf("Marshall Error: %v", marshErr)
			}
		} else {
			// The client will automatically try to recover from all errors.
			log.Printf("Consumer error: %v (%v)\n", err, msg)
		}
	}
}
