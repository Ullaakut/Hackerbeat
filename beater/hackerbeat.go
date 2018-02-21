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

// URL on which to request for all top stories at the moment from the HackerNews API
const getTopStoriesURL = "https://hacker-news.firebaseio.com/v0/topstories.json?print=pretty"

// Token to be replaced with storyID in getItemURL string
const storyIDToken = "$STORY_ID"

// URL on which to request a specific entity from the HackerNews API
var getItemURL = fmt.Sprintf("https://hacker-news.firebaseio.com/v0/item/%s.json?print=pretty", storyIDToken)

// Hackerbeat is an Elastic Beat that indexes HackerNews posts
type Hackerbeat struct {
	done   chan struct{}
	logger *logp.Logger
	config config.Config
	client beat.Client

	httpClient *http.Client
}

// A story reprensents a HackerNews entity that doesn't contain text and points to an external URL
type story struct {
	// The story unique entity identifier (all entities in HN share the same counter for identifiers)
	ID uint `json:"id"`

	// The score the entity was given by the HN community (one upvote means +1, one downvote means -1, stories start with 1 score)
	Score uint `json:"score"`

	// The time at which the story was created
	Time uint `json:"time"`

	// The story title
	Title string `json:"title"`

	// The author of the person who shared the story on HackerNews
	By string `json:"by"`

	// The external URL that the story points to (usually an article or a PDF document)
	URL string `json:"url"`
}

// New instanciates a new Hackerbeat using the given configuration
func New(b *beat.Beat, cfg *common.Config) (beat.Beater, error) {
	logger := logp.NewLogger("Hackerbeat")

	c := config.DefaultConfig
	if err := cfg.Unpack(&c); err != nil {
		return nil, errors.Wrap(err, "error reading config file")
	}

	bt := &Hackerbeat{
		done:   make(chan struct{}),
		logger: logger,
		config: c,
	}

	// Print configuration
	logger.Infow(
		"successfully created Hackerbeat instance with the following configuration:",
		"period", c.Period.String(),
		"timeout", c.Timeout.String(),
		"number_of_stories", c.NumberOfStories,
	)
	return bt, nil
}

// Run starts the Hackerbeat
func (bt *Hackerbeat) Run(b *beat.Beat) error {
	bt.logger.Info("hackerbeat is running! Hit CTRL-C to stop it.")

	// Connect to the publisher
	var err error
	bt.client, err = b.Publisher.Connect()
	if err != nil {
		return errors.Wrap(err, "failed to connect to publisher")
	}

	bt.httpClient = &http.Client{}

	ticker := time.NewTicker(bt.config.Period)
	for {
		select {
		case <-bt.done:
			return nil
		case <-ticker.C:
			stories, err := bt.fetchStories()
			if err != nil {
				bt.logger.Warnw(
					"failed to fetch posts from HackerNews",
					"error", err,
				)
			}

			bt.publishStories(b.Info.Name, stories)
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
	resp, err := bt.httpClient.Get(getTopStoriesURL)
	if err != nil {
		return nil, errors.Wrap(err, "could not get top stories from HackerNews API")
	}

	bytes, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, errors.Wrap(err, "could not read top stories response from HackerNews API")
	}

	var storyIDs []uint
	err = json.Unmarshal(bytes, &storyIDs)
	if err != nil {
		return nil, errors.Wrap(err, "could not parse top stories response from HackerNews API")
	}

	stories := make(chan story)
	for _, storyID := range storyIDs[0:bt.config.NumberOfStories] {
		go bt.fetchStory(stories, storyID)
	}

	timeout := time.NewTicker(bt.config.Timeout)
	var list []story
	// Until we reach timeout value, collect all stories from child goroutines
	for {
		select {
		case story := <-stories:
			list = append(list, story)
			// If all stories have been retreived, return list of stories
			if len(list) == bt.config.NumberOfStories {
				return list, nil
			}
		case <-timeout.C:
			// If timeout is reached, return all retrieved stories and warn the user
			// that timeout was reached
			bt.logger.Warnw(
				"timeout reached when fetching stories",
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

	// Generate getStoryURL from getItemURL and storyID
	getStoryURL := strings.Replace(getItemURL, storyIDToken, strconv.FormatUint(uint64(storyID), 10), -1)

	// Get story from HackerNews API
	resp, err := bt.httpClient.Get(getStoryURL)
	if err != nil {
		bt.logger.Errorw(
			"Failed to get story",
			"error", err,
		)
		return
	}

	// Read all bytes from response body
	storyInfos, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		bt.logger.Errorw(
			"Failed to parse story",
			"error", err,
		)
		return
	}

	// Unmarshal response body into our story data structure
	err = json.Unmarshal(storyInfos, &story)
	if err != nil {
		bt.logger.Errorw(
			"Failed to unmarshal story",
			"error", err,
		)
		return
	}

	// Send story back to the main thread
	stories <- story
}

func (bt *Hackerbeat) publishStories(beatName string, stories []story) {
	for _, story := range stories {
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
