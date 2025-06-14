package rabbitmq

import (
	"context"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/yyle88/erero"
	"github.com/yyle88/must"
	"go.uber.org/zap"
)

type HandleFunc func(name string, data []byte) error

// ConsumeMessage 启动 rabbitmq 手动ack模式
func ConsumeMessage(ctx context.Context, cfg *Config, handleFunc HandleFunc, maxRoutine int) error {
	var rateLimit = make(chan struct{}, maxRoutine) // 限制 go routine 的最大数量
	var onceLimit sync.Once
	var waitGroup sync.WaitGroup
	var outReason ReasonType
	waitGroup.Add(1) //在线程内部的某处会完成它
	go func() {
		for {
			reason, err := consumeMessage(ctx, cfg, handleFunc, &onceLimit, &waitGroup, rateLimit)
			if err != nil {
				must.Nice(reason)
				//假如已经成功初始化，这里就进不来，而只要能进来就必然是未初始化的
				onceLimit.Do(func() {
					outReason = must.Sane(reason, MqCannotInit)
					waitGroup.Done()
				})
				//假如首次连接就失败，就不重试连接，因为首次错误往往是配置问题，需要把错误返回给上层让他们选择
				if outReason == MqCannotInit {
					return
				}

				switch reason {
				case ContextIsDone:
					log.ErrorLog("return-ctx-is-done", zap.String("queue_name", cfg.QueueName), zap.String("reason", string(reason)))
					return
				case MqCannotInit:
					log.ErrorLog("can-not-init-queue", zap.String("queue_name", cfg.QueueName), zap.String("reason", string(reason)), zap.Error(err))
					time.Sleep(time.Second * 5)
					continue
				case MqDisconnected:
					log.ErrorLog("mq-is-disconnected", zap.String("queue_name", cfg.QueueName), zap.String("reason", string(reason)), zap.Error(err))
					time.Sleep(time.Second * 5)
					continue
				}
			}
		}
	}()
	log.DebugLog("waiting-rabbit-mq-init", zap.String("queue_name", cfg.QueueName))
	waitGroup.Wait() //前面是 go routine，但首次初始化还是要确认它能成功，因此这里会等待确认第一次连接成功
	log.DebugLog("rabbit-mq-init-success", zap.String("queue_name", cfg.QueueName))
	if outReason == MqCannotInit {
		return erero.New(string(MqCannotInit))
	}
	must.Zero(outReason)
	return nil
}

type ReasonType string

const (
	ContextIsDone  ReasonType = "context-is-done"
	MqCannotInit   ReasonType = "mq-cannot-init"
	MqDisconnected ReasonType = "mq-disconnected"
)

func consumeMessage(ctx context.Context, cfg *Config, handleFunc HandleFunc, onceLimit *sync.Once, waitGroup *sync.WaitGroup, rateLimit chan struct{}) (ReasonType, error) {
	conn, err := NewConn(cfg)
	if err != nil {
		return MqCannotInit, erero.Wro(err)
	}
	defer wrapClose(conn.Close)

	amqpChan, err := conn.Channel()
	if err != nil {
		return MqCannotInit, erero.Wro(err)
	}
	defer wrapClose(amqpChan.Close)

	queue, err := QueueDeclare(amqpChan, NewQueueConfig(cfg.QueueName))
	if err != nil {
		return MqCannotInit, erero.Wro(err)
	}

	deliveries, err := Consume(amqpChan, NewConsumeConfig(queue.Name))
	if err != nil {
		return MqCannotInit, erero.Wro(err)
	}

	onceLimit.Do(func() {
		log.DebugLog("rabbit-mq-once-success", zap.String("queue_name", cfg.QueueName))
		waitGroup.Done()
	})

	for {
		select {
		case <-ctx.Done():
			return ContextIsDone, erero.New(string(ContextIsDone))
		case <-time.After(time.Minute):
			log.DebugLog("waiting-MQ-message")
			continue
		case msg, ok := <-deliveries:
			if !ok {
				return MqDisconnected, erero.New(string(MqDisconnected))
			}
			goroutineRun(rateLimit, func() {
				//目前这个是 mq 的队列名称
				queueName := msg.RoutingKey
				//首个参数是 mq 的队列名称
				if err := handleFunc(queueName, msg.Body); err != nil {
					//这里不打印日志，由业务端确定是否需要打日志
					//在消费函数中，我们应该使用`Reject`函数将消息重新排队，或使用`Ack`函数确认消息已被成功处理。
					//这两个函数是互斥的，不能同时使用。
					if err := msg.Reject(true); err != nil {
						log.ErrorLog("can-not-reject-message", zap.String("queue_name", queueName), zap.Error(err))
						return //出错也没关系的，因为数据还在mq里，会再次被消费
					}
				} else {
					// 当没有错误时就确认消息，这两个函数 `Reject` 和 `Ack` 是互斥的，只有确认以后MQ才会认为已消费
					if err := msg.Ack(false); err != nil {
						log.ErrorLog("can-not-ack-message", zap.String("queue_name", queueName), zap.Error(err))
						return //出错也没关系的，因为数据还在mq里，会再次被消费
					}
				}
			})
		}
	}
}

func wrapClose(cleanup func() error) {
	if err := cleanup(); err != nil {
		log.ErrorLog("can-not-wrap-close", zap.Error(err))
		return
	}
}

type QueueConfig struct {
	Name       string
	Durable    bool
	AutoDelete bool
	Exclusive  bool
	NoWait     bool
	Args       amqp.Table
}

func NewQueueConfig(queueName string) *QueueConfig {
	return &QueueConfig{
		Name:       queueName, // name
		Durable:    true,      // durable 是否持久化。true：队列会被持久化到磁盘 false：队列只存在于内存中
		AutoDelete: false,     // delete when unused
		Exclusive:  false,     // exclusive
		NoWait:     false,     // no-wait
		Args:       nil,       // arguments
	}
}

func QueueDeclare(ch *amqp.Channel, cfg *QueueConfig) (*amqp.Queue, error) {
	queue, err := ch.QueueDeclare(cfg.Name, cfg.Durable, cfg.AutoDelete, cfg.Exclusive, cfg.NoWait, cfg.Args)
	if err != nil {
		return nil, erero.Wro(err)
	}
	return &queue, nil
}

type ConsumeConfig struct {
	Queue     string
	Consumer  string
	AutoAck   bool
	Exclusive bool
	NoLocal   bool
	NoWait    bool
	Args      amqp.Table
}

func NewConsumeConfig(queueName string) *ConsumeConfig {
	return &ConsumeConfig{
		Queue:     queueName, // queue
		Consumer:  "",        // consumer
		AutoAck:   false,     // auto-ack
		Exclusive: false,     // exclusive
		NoLocal:   false,     // no-local
		NoWait:    false,     // no-wait
		Args:      nil,       // args
	}
}

func Consume(ch *amqp.Channel, cfg *ConsumeConfig) (<-chan amqp.Delivery, error) {
	deliveries, err := ch.Consume(cfg.Queue, cfg.Consumer, cfg.AutoAck, cfg.Exclusive, cfg.NoLocal, cfg.NoWait, cfg.Args)
	if err != nil {
		return nil, erero.Wro(err)
	}
	return deliveries, nil
}

func goroutineRun(rateLimit chan struct{}, run func()) {
	rateLimit <- struct{}{}
	go func() {
		defer func() {
			<-rateLimit
		}()
		run()
	}()
}
