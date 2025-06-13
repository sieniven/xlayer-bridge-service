package messagepush

import (
	"fmt"

	"github.com/0xPolygonHermez/zkevm-bridge-service/log"
	"github.com/IBM/sarama"
)

var (
	notifier_topic = ""
)

func SetNotifierTopic(topic string) {
	notifier_topic = topic
	log.Infow("Set notifier topic", "topic", notifier_topic)
}

// Used to send messages to internal teams.
// This to ensure we publish to a different topic than the one used.
func (p *kafkaProducerImpl) Notify(msg interface{}) error {
	msgString, err := convertMsgToString(msg)
	if err != nil {
		return err
	}

	produceMsg := &sarama.ProducerMessage{
		Topic: notifier_topic,
		Value: sarama.StringEncoder(msgString),
	}

	partition, offset, err := p.producer.SendMessage(produceMsg)

	log.Debugf("Send notification to Kafka: topic[%v] msg[%v] partition[%v] offset[%v]", notifier_topic, msgString, partition, offset)
	return nil
}

// Small wrapper to expose private method
func Notify(producer KafkaProducer, msg interface{}) error {
	if producer == nil {
		return fmt.Errorf("Notifier producer is nil")
	}
	if notifier_topic == "" {
		return fmt.Errorf("Notifier topic is empty.")
	}
	p, ok := producer.(*kafkaProducerImpl)
	if !ok {
		return fmt.Errorf("Fail to cast kafkaProducerImpl for Notifier")
	}
	return p.Notify(msg)
}
