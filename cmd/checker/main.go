package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/joho/godotenv"
	"github.com/redis/go-redis/v9"
)

const (
	StreamPrefix  = "website-checks"
	ResultsStream = "website-check-results"
	GroupName     = "checkers"
)

func main() {
	err := godotenv.Load()
	if err != nil {
		log.Fatal(err)
	}

	regionCode := os.Getenv("REGION_CODE")
	if regionCode == "" {
		log.Fatal("REGION_CODE env var is required")
	}

	streamName := fmt.Sprintf("%s:%s", StreamPrefix, regionCode)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
		Protocol: 2,
	})
	defer rdb.Close()

	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Fatal(err)
	}

	err = rdb.XGroupCreateMkStream(ctx, streamName, GroupName, "0").Err()
	if err != nil && !isBusyGroup(err) {
		log.Fatal(err)
	}

	consumerID := uuid.New().String()
	log.Printf("worker starting: stream=%s, group=%s, consumer=%s", streamName, GroupName, consumerID)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		log.Println("shutting down...")
		cancel()
	}()

	for {
		select {
		case <-ctx.Done():
			log.Println("worker stopped")
			return
		default:
		}

		streams, err := rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    GroupName,
			Consumer: consumerID,
			Streams:  []string{streamName, ">"},
			Count:    10,
			Block:    5 * time.Second,
		}).Result()
		if err != nil {
			if errors.Is(err, context.Canceled) {
				log.Println("worker stopped")
				return
			}
			if errors.Is(err, redis.Nil) {
				continue
			}
			log.Printf("error reading from stream: %v", err)
			continue
		}

		for _, stream := range streams {
			for _, msg := range stream.Messages {
				websiteIDStr, _ := msg.Values["website_id"].(string)
				regionIDStr, _ := msg.Values["region_id"].(string)
				url, _ := msg.Values["url"].(string)

				websiteID, err := uuid.Parse(websiteIDStr)
				if err != nil {
					log.Printf("msg %s: invalid website_id %q: %v – skipping", msg.ID, websiteIDStr, err)
					continue
				}
				regionID, err := uuid.Parse(regionIDStr)
				if err != nil {
					log.Printf("msg %s: invalid region_id %q: %v – skipping", msg.ID, regionIDStr, err)
					continue
				}
				if url == "" {
					log.Printf("msg %s: empty url – skipping", msg.ID)
					continue
				}

				result := checkWebsite(websiteID, regionID, url)
				log.Printf("msg %s: website_id=%s region_id=%s url=%s status=%s latency=%dms checked_at=%s",
					msg.ID,
					result.WebsiteID,
					result.RegionID,
					url,
					result.Status,
					result.ResponseTimeMs,
					result.CheckedAt.Format(time.RFC3339),
				)

				// Publish the result event BEFORE acknowledging.
				// If XADD fails the job stays pending in the PEL and will be
				// retried on the next XReadGroup with "0" (reclaim pending).
				if err := publishResult(ctx, rdb, result); err != nil {
					log.Printf("msg %s: failed to publish result: %v – will retry", msg.ID, err)
					continue
				}

				// Only acknowledge once the result is durably in the stream.
				if err := rdb.XAck(ctx, streamName, GroupName, msg.ID).Err(); err != nil {
					log.Printf("msg %s: XACK failed: %v", msg.ID, err)
				}
			}
		}
	}
}

func isBusyGroup(err error) bool {
	return strings.Contains(err.Error(), "BUSYGROUP")
}
