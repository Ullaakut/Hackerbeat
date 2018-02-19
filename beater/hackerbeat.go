package beater

import (
	"fmt"
	"time"

	"github.com/elastic/beats/libbeat/beat"
	"github.com/elastic/beats/libbeat/common"
	"github.com/elastic/beats/libbeat/logp"

	"github.com/Ullaakut/hackerbeat/config"
)

// Hackerbeat is an Elastic Beat that indexes HackerNews posts
type Hackerbeat struct {
	done   chan struct{}
	config config.Config
	client beat.Client

	busy bool
}

type post struct {
	id      uint
	content []byte
}

// New instanciates a new Hackerbeat using the given configuration
func New(b *beat.Beat, cfg *common.Config) (beat.Beater, error) {
	c := config.DefaultConfig
	if err := cfg.Unpack(&c); err != nil {
		return nil, fmt.Errorf("Error reading config file: %v", err)
	}

	bt := &Hackerbeat{
		done:   make(chan struct{}),
		config: c,
	}
	return bt, nil
}

// Run starts the Hackerbeat
func (bt *Hackerbeat) Run(b *beat.Beat) error {
	logp.Info("hackerbeat is running! Hit CTRL-C to stop it.")

	var err error
	bt.client, err = b.Publisher.Connect()
	if err != nil {
		return err
	}

	ticker := time.NewTicker(bt.config.Period)
	for {
		select {
		case <-bt.done:
			return nil
		case <-ticker.C:
			if !bt.busy {
				posts, err := bt.fetchPosts()
				if err != nil {
					logp.Warn(
						"Failed to fetch posts from HackerNews",
						logp.Error(err),
					)
				}

				for _, post := range posts {
					event := beat.Event{
						Timestamp: time.Now(),
						Fields: common.MapStr{
							"type": b.Info.Name,
							"id":   post.id,
							"data": post.content,
						},
					}
					bt.client.Publish(event)
					logp.Info("Event sent")
				}
			}
		}
	}
}

// Stop stops the Hackerbeat
func (bt *Hackerbeat) Stop() {
	bt.client.Close()
	close(bt.done)
}

func (bt *Hackerbeat) fetchPosts() ([]post, error) {
	bt.busy = true
	defer func() {
		bt.busy = false
	}()

	// Implement logic of fetching posts from HN API
	return nil, nil
}
