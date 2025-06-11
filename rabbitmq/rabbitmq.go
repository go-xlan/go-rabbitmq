package rabbitmq

import (
	"crypto/tls"
	"fmt"
	"net/url"

	"github.com/pkg/errors"
	amqp "github.com/rabbitmq/amqp091-go"
)

type Config struct {
	Protocol  string
	Host      string
	Username  string
	Password  string
	Port      int
	QueueName string
}

func NewConn(cfg *Config) (*amqp.Connection, error) {
	connURL := NewConnURL(cfg)

	switch cfg.Protocol {
	case "amqp":
		return amqp.Dial(connURL)
	case "amqps":
		return amqp.DialTLS(connURL, &tls.Config{MinVersion: tls.VersionTLS12})
	default:
		return nil, errors.Errorf("unknown protocol type=%v", cfg.Protocol)
	}
}

func NewConnURL(cfg *Config) string {
	username := url.QueryEscape(cfg.Username)
	password := url.QueryEscape(cfg.Password)
	return fmt.Sprintf("%s://%s:%s@%s:%d/", cfg.Protocol, username, password, cfg.Host, cfg.Port)
}
