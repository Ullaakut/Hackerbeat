package beater

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/elastic/beats/libbeat/beat"
	"github.com/elastic/beats/libbeat/common"
	"github.com/elastic/beats/libbeat/logp"
	"github.com/pkg/errors"

	"github.com/Ullaakut/hackerbeat/config"
)

const storyIDToken = "$STORY_ID"
const getItemURL = "https://hacker-news.firebaseio.com/v0/item/$STORY_ID.json?print=pretty"
const getTopStoriesURL = "https://hacker-news.firebaseio.com/v0/topstories.json?print=pretty"

// Hackerbeat is an Elastic Beat that indexes HackerNews posts
type Hackerbeat struct {
	done   chan struct{}
	logger *logp.Logger
	config config.Config
	client beat.Client

	httpClient *http.Client

	busy bool
}

type story struct {
	ID    uint   `json:"id"`
	Score uint   `json:"score"`
	Time  uint   `json:"time"`
	Title string `json:"title"`
	By    string `json:"by"`
	URL   string `json:"url"`

	err error
}

// New instanciates a new Hackerbeat using the given configuration
func New(b *beat.Beat, cfg *common.Config) (beat.Beater, error) {
	logger := logp.NewLogger("Hackerbeat")

	c := config.DefaultConfig
	if err := cfg.Unpack(&c); err != nil {
		return nil, fmt.Errorf("Error reading config file: %v", err)
	}

	bt := &Hackerbeat{
		done:   make(chan struct{}),
		logger: logger,
		config: c,
	}

	logger.Infow(
		"Successfully created Hackerbeat instance with the following configuration:",
		"period", c.Period,
		"timeout", c.Timeout,
		"number_of_stories", c.NumberOfStories,
	)
	return bt, nil
}

// Run starts the Hackerbeat
func (bt *Hackerbeat) Run(b *beat.Beat) error {
	bt.logger.Info("hackerbeat is running! Hit CTRL-C to stop it.")

	var err error
	bt.client, err = b.Publisher.Connect()
	if err != nil {
		return err
	}

	bt.httpClient = &http.Client{}

	ticker := time.NewTicker(bt.config.Period)
	for {
		select {
		case <-bt.done:
			return nil
		case <-ticker.C:
			if !bt.busy {
				stories, err := bt.fetchStories()
				if err != nil {
					bt.logger.Warnw(
						"Failed to fetch posts from HackerNews",
						"error", err,
					)
				}

				bt.publishStories(b.Info.Name, stories)
			}
		}
	}
}

// Stop stops the Hackerbeat
func (bt *Hackerbeat) Stop() {
	bt.client.Close()
	close(bt.done)
}

// Fetch top stories from the Hacker News API
func (bt *Hackerbeat) fetchStories() ([]story, error) {
	bt.busy = true
	defer func() {
		bt.busy = false
	}()

	resp, err := bt.httpClient.Get(getTopStoriesURL)
	if err != nil {
		return nil, err
	}

	bytes, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var storyIDs []uint
	err = json.Unmarshal(bytes, &storyIDs)
	if err != nil {
		return nil, err
	}

	stories := make(chan story)
	for _, storyID := range storyIDs[0:bt.config.NumberOfStories] {
		go bt.fetchStory(stories, storyID)
	}

	timeout := time.NewTicker(bt.config.Timeout)
	var list []story
	for {
		select {
		case story := <-stories:
			list = append(list, story)
			if len(list) == bt.config.NumberOfStories {
				return list, nil
			}
		case <-timeout.C:
			bt.logger.Warnw(
				"Timeout reached when fetching stories",
				"timeout_value", bt.config.Timeout.String(),
			)
			return list, nil
		}
	}
}

// Fetch a top story's details
func (bt *Hackerbeat) fetchStory(stories chan<- story, storyID uint) {
	story := story{
		ID: storyID,
	}

	getThisItemURL := strings.Replace(getItemURL, storyIDToken, strconv.FormatUint(uint64(storyID), 10), -1)
	resp, err := bt.httpClient.Get(getThisItemURL)
	if err != nil {
		story.err = errors.Wrap(err, "Failed to get story")
		stories <- story
		return
	}

	storyInfos, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		story.err = errors.Wrap(err, "Failed to parse story")
		stories <- story
		return
	}

	err = json.Unmarshal(storyInfos, &story)
	if err != nil {
		story.err = errors.Wrap(err, "Failed to unmarshal story")
	}

	stories <- story
}

func (bt *Hackerbeat) publishStories(beatName string, stories []story) {
	for _, story := range stories {
		if story.err != nil {
			bt.logger.Errorw(
				"Could not fetch story",
				"id", story.ID,
				"error", story.err,
			)
		} else {
			event := beat.Event{
				Timestamp: time.Now(),
				Fields: common.MapStr{
					"type":         beatName,
					"story_id":     story.ID,
					"story_score":  story.Score,
					"story_time":   story.Time,
					"story_title":  story.Title,
					"story_author": story.By,
					"story_url":    story.URL,
				},
			}
			bt.client.Publish(event)
			bt.logger.Infow(
				"Published story",
				"id", story.ID,
				"title", story.Title,
			)
		}
	}
}
