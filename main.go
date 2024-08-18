package main

import (
	"github.com/donghquinn/kafka-cdc/configs"
	"github.com/donghquinn/kafka-cdc/lib"
	"github.com/joho/godotenv"
)

func main() {
	godotenv.Load(".env")
	configs.SetKafkaConfig()
	
	lib.Consumer()
}