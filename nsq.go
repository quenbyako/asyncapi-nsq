package nsq

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/lerenn/asyncapi-codegen/pkg/extensions"
	"github.com/lerenn/asyncapi-codegen/pkg/extensions/brokers"
	"github.com/nsqio/go-nsq"
)

const defaultChannelName = "default"

type Controller struct {
	addr    string
	p       *nsq.Producer
	logger  extensions.Logger
	connect func(c *nsq.Consumer, addr string) error
}

var _ extensions.BrokerController = (*Controller)(nil)

// ControllerOption is a function that can be used to configure a NSQ controller
// Examples: WithQueueGroup(), WithLogger().
type ControllerOption func(controller *Controller)

// NewController creates a new NSQ controller.
func NewController(url string, options ...ControllerOption) (*Controller, error) {
	p, err := nsq.NewProducer(url, nsq.NewConfig())
	if err != nil {
		return nil, err
	}

	c := &Controller{
		addr:    url,
		p:       p,
		logger:  extensions.DummyLogger{},
		connect: nsqdConnect,
	}

	// Execute options
	for _, option := range options {
		option(c)
	}

	return c, nil
}

// WithLogger set a custom logger that will log operations on broker controller.
func WithLogger(logger extensions.Logger) ControllerOption {
	return func(controller *Controller) { controller.logger = logger }
}

func WithLookupdConnect() ControllerOption {
	return func(controller *Controller) { controller.connect = nsqlookupdConnect }
}

// Publish a message to the broker.
func (c *Controller) Publish(_ context.Context, topic string, bm extensions.BrokerMessage) error {
	if i := strings.IndexRune(topic, '#'); i >= 0 {
		topic = topic[:i]
	}

	return c.p.Publish(topic, bm.Payload)
}

// Subscribe to messages from the broker.
func (c *Controller) Subscribe(ctx context.Context, topic string) (extensions.BrokerChannelSubscription, error) {
	channel := defaultChannelName
	if i := strings.IndexRune(topic, '#'); i >= 0 && i < len(topic)-1 {
		channel = topic[i+1:]
		topic = topic[:i]
	}

	consumer, err := nsq.NewConsumer(topic, channel, nsq.NewConfig())
	if err != nil {
		return extensions.BrokerChannelSubscription{}, err
	}

	msgChan := make(chan extensions.BrokerMessage, brokers.BrokerMessagesQueueSize)
	consumer.AddHandler(messagesHandler(msgChan))

	if err := c.connect(consumer, c.addr); err != nil {
		return extensions.BrokerChannelSubscription{}, err
	}

	// Create a new subscription
	sub := extensions.NewBrokerChannelSubscription(msgChan, make(chan any, 1))
	sub.WaitForCancellationAsync(consumer.Stop)

	return sub, nil
}

func (c *Controller) LookupTopics(ctx context.Context) ([]string, error) {
	endpoint := (&url.URL{
		Scheme: "http",
		Host:   c.addr,
		Path:   "/topics",
	}).String()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, fmt.Errorf("creating request: %w", err)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("trying to get list of topics from nsqlookupd: %w", err)
	}

	type topicsBody struct {
		Topics []string `json:"topics"`
	}

	var body topicsBody
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return nil, fmt.Errorf("trying to parse response from nsqlookupd: %w", err)
	}

	return body.Topics, nil
}

func messagesHandler(c chan<- extensions.BrokerMessage) nsq.Handler {
	return nsq.HandlerFunc(func(message *nsq.Message) error {
		headers := map[string][]byte{
			"X-MsgID":     []byte(message.ID[:]),
			"X-Attempts":  []byte(strconv.Itoa(int(message.Attempts))),
			"X-Timestamp": []byte(strconv.Itoa(int(message.Timestamp))),
		}

		c <- extensions.BrokerMessage{
			Headers: headers,
			Payload: message.Body,
		}

		return nil
	})
}

// Close closes everything related to the broker.
func (c *Controller) Close() { c.p.Stop() }

func nsqdConnect(c *nsq.Consumer, addr string) error       { return c.ConnectToNSQD(addr) }
func nsqlookupdConnect(c *nsq.Consumer, addr string) error { return c.ConnectToNSQLookupd(addr) }
