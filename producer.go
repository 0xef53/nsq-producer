package producer

import (
	"encoding/json"
	"log"
	"os"

	"github.com/nsqio/go-nsq"
)

type logger interface {
	Output(int, string) error
}

// Producer inherets the nsq.Producer object
type Producer struct {
	Log      logger
	LogLevel nsq.LogLevel

	*nsq.Producer
}

func NewProducer(addr string, config *nsq.Config) (p *Producer, err error) {
	p = &Producer{
		Log:      log.New(os.Stderr, "", log.LstdFlags),
		LogLevel: nsq.LogLevelInfo,
	}

	p.Producer, err = nsq.NewProducer(addr, config)
	if err != nil {
		return nil, err
	}
	p.Producer.SetLogger(p.Log, p.LogLevel)

	return p, nil
}

// PublishJSON sends message to the NSQ topic in the JSON format
func (p *Producer) PublishJSON(topic string, v interface{}) error {
	body, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return p.Publish(topic, body)
}

// PublishJSONAsync sends message to the NSQ topic in the JSON format
// asynchronously
func (p *Producer) PublishJSONAsync(topic string, v interface{}, doneChan chan *nsq.ProducerTransaction, args ...interface{}) error {
	body, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return p.PublishAsync(topic, body, doneChan, args...)
}
