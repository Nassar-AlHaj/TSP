package com.twitterstreampipeline.config

case class KafkaConfig(
                        bootstrapServers: String,
                        topic: String,
                        clientId: String = "twitter-producer",
                        batchSize: String = "16384",
                        bufferMemory: String = "33554432"
                      )