package messaging

import (
	"context"
	"encoding/json"
	amqp "github.com/rabbitmq/amqp091-go"
	"time"
)

type MessageBusClient interface {
	EstablishConnection()
	Consume(topic string, options *ConsumeOptions) (<-chan *MessageEvent, error)
	GetDSN() string
	GetEngine() interface{}
	Publish(topic string, message []byte, options *PublishOptions) (bool, error)
}

type BusEventResponse struct {
}

func (b BusEventResponse) String() string {
	jb, _ := json.Marshal(b)
	return string(jb)
}

type MessageEvent struct {
	Action       string            `json:"action,omitempty"`
	Application  string            `json:"application,omitempty"`
	Event        string            `json:"event,omitempty"`
	Metadata     map[string]any    `json:"metadata,omitempty"`
	Medium 		 []string 		   `json:"medium"`
	Payload      []byte            `json:"payload,omitempty"`
	Timestamp    int64             `json:"timestamp,omitempty"`
	Acknowledger amqp.Acknowledger `json:"-"`
	Tag          uint64            `json:"-"`
}

type PublishOptions struct {
	Args        map[string]interface{} `json:"args,omitempty"`
	AutoDelete  bool                   `json:"auto_delete,omitempty"`
	ContentType string                 `json:"content_type,omitempty"`
	Durable     bool                   `json:"durable,omitempty"`
	Exclusive   bool                   `json:"exclusive,omitempty"`
	Exchange    string                 `json:"exchange,omitempty"`
	Mandatory   bool                   `json:"mandatory,omitempty"`
	NoWait      bool                   `json:"no_wait,omitempty"`
	Immediately bool                   `json:"immediately,omitempty"`
}

type ConsumeOptions struct {
	Args         map[string]interface{} `json:"args,omitempty"`
	AutoAck      bool                   `json:"auto_ack,omitempty"`
	AutoDelete   bool                   `json:"auto_delete,omitempty"`
	ConsumerName string                 `json:"consumer_name,omitempty"`
	Exchange     string                 `json:"exchange,omitempty"`
	QueueName    string                 `json:"queue_name,omitempty"`
	Durable      bool                   `json:"durable,omitempty"`
	Exclusive    bool                   `json:"exclusive,omitempty"`
	NoLocal      bool                   `json:"no_local,omitempty"`
	NoWait       bool                   `json:"no_wait,omitempty"`
}

func New(ctx context.Context) MessageBusClient {
	return NewRabbitMQClient(ctx, nil)
}

type RabbitMQConfig struct {
	Host   string        `mapstructure:"host"`
	Port   string        `mapstructure:"port"`
	Queue  string        `mapstructure:"queue"`
	Scheme string        `mapstructure:"scheme"`
	Auth   *RabbitMqAuth `mapstructure:"auth"`
}

type RabbitMqAuth struct {
	Username string `mapstructure:"username"`
	Password string `mapstructure:"password"`
}

func NewEvent(application, event, action string) *MessageEvent {
	return &MessageEvent{
		Action:      action,
		Application: application,
		Event:       event,
		Metadata:    nil,
		Payload:     nil,
		Timestamp:   time.Now().UnixMilli(),
		Medium: 	 []string{"email"},
	}
}

func (e MessageEvent) String() string {
	jb, _ := json.Marshal(e)
	return string(jb)
}

func (receiver RabbitMQConfig) String() string {
	jb, _ := json.Marshal(receiver)
	return string(jb)
}
