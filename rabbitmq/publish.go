package rabbitmq

import (
	"context"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/yyle88/erero"
)

type PublishClient struct {
	conn     *amqp.Connection
	amqpChan *amqp.Channel
	queue    *amqp.Queue
	ctx      context.Context
}

func NewPublishClient(ctx context.Context, cfg *Config) (*PublishClient, error) {
	conn, err := NewConn(cfg)
	if err != nil {
		return nil, erero.Wro(err)
	}
	amqpChan, err := conn.Channel()
	if err != nil {
		return nil, erero.Wro(err)
	}
	queue, err := QueueDeclare(amqpChan, NewQueueConfig(cfg.QueueName))
	if err != nil {
		return nil, erero.Wro(err)
	}
	client := &PublishClient{
		conn:     conn,
		amqpChan: amqpChan,
		queue:    queue,
		ctx:      ctx,
	}
	return client, nil
}

func (p *PublishClient) Close() error {
	if err := p.amqpChan.Close(); err != nil {
		return erero.Wro(err)
	}
	if err := p.conn.Close(); err != nil {
		return erero.Wro(err)
	}
	return nil
}

func (p *PublishClient) PublishMessage(ctx context.Context, data []byte) error {
	arg := newPublishParam(p.queue.Name)
	err := p.amqpChan.PublishWithContext(
		ctx,
		arg.Exchange,
		arg.RoutingKey,
		arg.Mandatory,
		arg.Immediate,
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        data,
		},
	)
	if err != nil {
		return erero.Wro(err)
	}
	log.DebugLog("publish-message-success")
	return nil
}

type publishParam struct {
	Exchange   string
	RoutingKey string
	Mandatory  bool
	Immediate  bool
}

func newPublishParam(queueName string) *publishParam {
	return &publishParam{
		Exchange:   "",
		RoutingKey: queueName,
		Mandatory:  false,
		Immediate:  false,
	}
}
