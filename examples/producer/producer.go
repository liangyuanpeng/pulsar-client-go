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
	"strconv"

	"github.com/apache/pulsar-client-go/pulsar"
)

func main() {

	pulsarHost := os.Getenv("pulsarHost")
	topic := os.Getenv("topic")
	countStr := os.Getenv("count")
	subscriptionName := os.Getenv("subscriptionName")

	count := 3

	if countStr != "" {
		countTmp, error := strconv.Atoi(countStr)
		if error != nil {
			fmt.Println("字符串转换成整数失败")
		} else {
			count = countTmp
		}
	}

	//pulsar://192.168.1.89:30552

	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL:            pulsarHost,
		Authentication: pulsar.NewAuthenticationToken("eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJ5dW5ob3JuIn0.c2IZY4z0i6ZBgzhePSFW0F5hBo2UTE9a5rvqLXvU6-Y"),
	})

	if err != nil {
		log.Fatal(err)
	}

	defer client.Close()

	producer, err := client.CreateProducer(pulsar.ProducerOptions{
		Topic: topic,
	})
	if err != nil {
		log.Fatal(err)
	}

	defer producer.Close()

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

	ctx := context.Background()

	for i := 0; i < count; i++ {
		if msgId, err := producer.Send(ctx, &pulsar.ProducerMessage{
			Payload: []byte(fmt.Sprintf("hello-%d", i)),
		}); err != nil {
			log.Fatal(err)
		} else {
			log.Println("Published message: ", msgId)
		}
	}
}
