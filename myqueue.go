package myqueue

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"
	"sync"
	"time"

	jsoniter "github.com/json-iterator/go"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/streadway/amqp"
)

var json = jsoniter.ConfigFastest

type Queue struct {
	queue       string
	dsn         string
	tag         string
	conn        *amqp.Connection
	ch          *amqp.Channel
	q           amqp.Queue
	connectedAt time.Time

	lock sync.Mutex
}

func New(isDeclare bool, consumerTag, queue, dsn string) (*Queue, error) {
	q := &Queue{dsn: dsn, queue: queue, tag: consumerTag}
	err := q.connect()
	if err != nil {
		return nil, err
	}
	q.lock = sync.Mutex{}

	if isDeclare {
		q.q, err = declare(q.ch, queue)
		if err != nil {
			closeErr := q.Close()
			if closeErr != nil {
				log.Err(err).Msg("close")
			}
			return nil, fmt.Errorf("declare queue: %w", err)
		}
	}

	return q, nil
}

// force delete queue
func (q *Queue) Delete(force bool) (int, error) {
	if force {
		return q.ch.QueueDelete(q.queue, false, false, false)
	}
	return q.ch.QueuePurge(q.queue, false)
}

func (q *Queue) Count() (int, error) {
	queue, err := q.ch.QueueInspect(q.queue)
	if err != nil {
		return 0, err
	}
	return queue.Messages, nil
}

func (q *Queue) SendRaw(priority int, body []byte) error {
	err := sendRaw(q.ch, q.q.Name, priority, body)
	if errors.Is(err, amqp.ErrClosed) {
		log.Info().Msg("rabbit closed, reconnect")
		err = q.reconnect()
		if err != nil {
			return err
		}

		return sendRaw(q.ch, q.queue, priority, body)
	}
	return err
}

func (q *Queue) Send(priority int, task interface{}) error {
	err := Send(q.ch, q.q.Name, priority, task)
	if errors.Is(err, amqp.ErrClosed) {
		err = q.reconnect()
		if err != nil {
			return err
		}

		return Send(q.ch, q.queue, priority, task)
	}
	return err
}

func (q *Queue) reconnect() error {
	closedAt := time.Now()
	q.lock.Lock()
	defer q.lock.Unlock()

	if closedAt.After(q.connectedAt) {
		err := q.Close()
		if err != nil {
			log.Err(err).Msg("close")
		}

		err = q.connect()
		if err != nil {
			return err
		}
	}

	return nil
}

func (q *Queue) Messages(prefetch int) (<-chan amqp.Delivery, error) {
	return consume(q.ch, q.tag, q.queue, prefetch)
}

var QueueClosedError = fmt.Errorf("queue closed")

type consumeFn func(m amqp.Delivery) (requeue bool, err error)

func (q *Queue) Consume(log zerolog.Logger, ctx context.Context, prefetch, workers int, f consumeFn) error {
	if workers == 0 {
		return fmt.Errorf("0 workers")
	}
	if prefetch == 0 {
		prefetch = workers * 2
	}
	msgs, err := consume(q.ch, q.tag, q.queue, prefetch)
	if err != nil {
		return fmt.Errorf("consume: %w", err)
	}

	worker(workers, func(ind int) {
		var ok bool
		for {
			// приоритеней чекнуть контекст
			if err := isCtxDone(ctx); err != nil {
				log.Info().Msg("context done")
				return
			}

			var m amqp.Delivery
			select {
			//на случай если нет тасок
			case <-ctx.Done():
				if ind == 0 {
					log.Info().Msg("context done")
				}
				return
			case m, ok = <-msgs:
				if !ok {
					if ind == 0 {
						log.Info().Msg("messages closed")
					}
					return
				}
			case <-time.After(1 * time.Minute):
				if ind == 0 {
					log.Debug().Msg("waiting tasks...")
				}
				continue
			}

			var requeue bool
			func() {
				defer func() {
					if r := recover(); r != nil {
						err = errors.New(fmt.Sprint(r))
						log.Info().
							Str("stack", string(debug.Stack())).
							Err(err).
							Msg("panic")
					}
					requeue = true
				}()
				requeue, err = f(m)
			}()
			if err != nil {
				requeue = requeue || isCtxDone(ctx) != nil
				log.Printf("worker error[%t]: %s\n", requeue, err)
				err := m.Nack(false, requeue)
				if err != nil {
					log.Err(err).Msg("nack error")
				}
			} else {
				err := m.Ack(false)
				if err != nil {
					log.Err(err).Msg("ack error")
				}
			}
		}
	}, nil)

	err = isCtxDone(ctx)
	if err != nil {
		return fmt.Errorf("%s: %w", q.queue, ctx.Err())
	} else {
		return fmt.Errorf("%s: %w", q.queue, QueueClosedError)
	}
}

func worker(count int, f func(index int), callback func()) {
	wg := sync.WaitGroup{}

	for i := 1; i <= count; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			f(i)
		}(i)
	}
	wg.Wait()

	if callback != nil {
		callback()
	}
}

func isCtxDone(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		return nil
	}
}

func (q *Queue) connect() (err error) {
	//lock in send
	q.conn, err = amqp.Dial(q.dsn)
	if err != nil {
		return err
	}

	q.ch, err = q.conn.Channel()
	if err != nil {
		closeErr := q.conn.Close()
		if closeErr != nil {
			log.Err(err).Msg("close connection")
		}
		return err
	}
	q.connectedAt = time.Now()
	return nil
}

func (q *Queue) Close() error {
	//var merr *multierror.Error
	_ = q.ch.Close()
	//merr = multierror.Append(merr, err)
	_ = q.conn.Close()
	//merr = multierror.Append(merr, err)
	//return merr.ErrorOrNil()
	return nil
}

// 10 - 1 = 9 для того чтобы можно было поставить таску с приоритетом 10.
// Её можно поставить только руками
const MaxPriority = 9

func Read(from []byte, to interface{}) error {
	iter := json.BorrowIterator(from)
	defer json.ReturnIterator(iter)
	iter.ReadVal(&to)
	if iter.Error != nil {
		return fmt.Errorf("unmarshal '%s': %w", from, iter.Error)
	}

	return nil
}

func sendRaw(ch *amqp.Channel, queueName string, priority int, body []byte) error {
	err := ch.Publish("", queueName, false, false,
		amqp.Publishing{
			Priority:     uint8(priority),
			DeliveryMode: amqp.Persistent,
			ContentType:  "application/json",
			Body:         body,
		},
	)
	if err != nil {
		return fmt.Errorf("publish task: %w", err)
	}
	return nil
}

func Send(ch *amqp.Channel, queueName string, priority int, task interface{}) error {
	body, err := json.Marshal(task)
	if err != nil {
		return fmt.Errorf("marshal task: %w", err)
	}
	return sendRaw(ch, queueName, priority, body)
}

func declare(c *amqp.Channel, name string) (amqp.Queue, error) {
	t := amqp.Table{
		"x-max-priority": 10,
	}

	q, err := c.QueueDeclare(name, true, false, false, false, t)
	if err != nil {
		return amqp.Queue{}, fmt.Errorf("declare queue '%s': %w", name, err)
	}

	return q, nil
}

func consume(c *amqp.Channel, tag, queue string, prefetch int) (<-chan amqp.Delivery, error) {
	err := c.Qos(prefetch, 0, false)
	if err != nil {
		return nil, fmt.Errorf("set prefetch %d for consume: %w", prefetch, err)
	}

	return c.Consume(
		queue,
		tag,
		false,
		false,
		false,
		false,
		nil,
	)
}
