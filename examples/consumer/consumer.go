// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
)

func doConsumer(consumer pulsar.Consumer, topic string, subscriptionName string) {

	worked["1"] = "1"

	time.Sleep(time.Duration(1) * time.Second)

	for i := 0; i < 1; i++ {
		msg, err := consumer.Receive(context.Background())
		if err != nil {
			log.Fatal(err)
		}

		fmt.Printf("Received message msgId: %#v -- content: '%s'\n",
			msg.ID(), string(msg.Payload()))

		consumer.Ack(msg)
	}

	fmt.Println("begin Unsubscribe")

	_, ok0 := worked["0"]
	if !ok0 {
		// if err := consumer.Unsubscribe(); err != nil {
		// 	log.Fatal(err)
		// }
	}

	delete(worked, "0")
	delete(worked, "1")
}

var worked = make(map[string]string)

func main() {

	pulsarHost := os.Getenv("pulsarHost")
	topic := os.Getenv("topic")
	subscriptionName := os.Getenv("subscriptionName")

	client, err := pulsar.NewClient(pulsar.ClientOptions{URL: pulsarHost})
	if err != nil {
		log.Fatal(err)
	}

	defer client.Close()

	consumer, err := client.Subscribe(pulsar.ConsumerOptions{
		Topic:             topic,
		SubscriptionName:  subscriptionName,
		Type:              pulsar.Shared,
		ReceiverQueueSize: 1,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer consumer.Close()

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	worked["0"] = "0"

	for {
		select {
		case <-ticker.C:
			_, ok := worked["1"]
			log.Println("********************worked:", worked)
			if !ok {
				go doConsumer(consumer, topic, subscriptionName)
			}

		}
	}

	// consumer, err := client.Subscribe(pulsar.ConsumerOptions{
	// 	Topic:             topic,
	// 	SubscriptionName:  subscriptionName,
	// 	Type:              pulsar.Shared,
	// 	ReceiverQueueSize: 1,
	// })
	// if err != nil {
	// 	log.Fatal(err)
	// }
	// defer consumer.Close()

	// for i := 0; i < 10; i++ {
	// 	msg, err := consumer.Receive(context.Background())
	// 	if err != nil {
	// 		log.Fatal(err)
	// 	}

	// 	fmt.Printf("Received message msgId: %#v -- content: '%s'\n",
	// 		msg.ID(), string(msg.Payload()))

	// 	consumer.Ack(msg)
	// }

	// if err := consumer.Unsubscribe(); err != nil {
	// 	log.Fatal(err)
	// }
}
