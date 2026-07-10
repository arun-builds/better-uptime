package main

import (
	"context"
	"errors"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/arun-builds/better-uptime/internal/db"
	"github.com/arun-builds/better-uptime/internal/store"
	"github.com/google/uuid"
	"github.com/joho/godotenv"
	"github.com/redis/go-redis/v9"
)

const (
	ResultsStream = "website-check-results"

	GroupName = "persistence-workers"
)

func main() {
	if err := godotenv.Load(); err != nil {
		log.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pool, err := db.NewPool(ctx, 1, 4)
	if err != nil {
		log.Fatalf("db pool: %v", err)
	}
	defer pool.Close()

	queries := store.New(pool)

	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
		Protocol: 2,
	})
	defer rdb.Close()

	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Fatalf("redis ping: %v", err)
	}

	err = rdb.XGroupCreateMkStream(ctx, ResultsStream, GroupName, "0").Err()
	if err != nil && !isBusyGroup(err) {
		log.Fatalf("create consumer group: %v", err)
	}

	consumerID := uuid.New().String()
	log.Printf("persistence worker starting: stream=%s, group=%s, consumer=%s",
		ResultsStream, GroupName, consumerID)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		log.Println("shutting down...")
		cancel()
	}()

	// Worker loop
	for {
		select {
		case <-ctx.Done():
			log.Println("persistence worker stopped")
			return
		default:
		}

		streams, err := rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    GroupName,
			Consumer: consumerID,
			Streams:  []string{ResultsStream, ">"},
			Count:    10,
			Block:    5 * time.Second,
		}).Result()
		if err != nil {
			if errors.Is(err, context.Canceled) {
				log.Println("persistence worker stopped")
				return
			}
			if errors.Is(err, redis.Nil) {
				continue
			}
			log.Printf("xreadgroup error: %v", err)
			continue
		}

		for _, stream := range streams {
			for _, msg := range stream.Messages {
				if err := handleMessage(ctx, queries, rdb, msg); err != nil {
					// Error already logged inside handleMessage.
					// Leave the message in the PEL; it will be retried.
					// TODO: limit the retries
					continue
				}

				if err := rdb.XAck(ctx, ResultsStream, GroupName, msg.ID).Err(); err != nil {
					log.Printf("msg %s: xack failed: %v", msg.ID, err)
				}
			}
		}
	}
}

func handleMessage(ctx context.Context, q *store.Queries, rdb *redis.Client, msg redis.XMessage) error {
	idStr, _ := msg.Values["id"].(string)
	websiteIDStr, _ := msg.Values["website_id"].(string)
	regionIDStr, _ := msg.Values["region_id"].(string)
	statusStr, _ := msg.Values["status"].(string)
	latencyStr, _ := msg.Values["response_time_ms"].(string)

	ID, err := uuid.Parse(idStr)
	if err != nil {
		log.Printf("msg %s: invalid id %q – skipping", msg.ID, websiteIDStr)
		return err
	}

	websiteID, err := uuid.Parse(websiteIDStr)
	if err != nil {
		log.Printf("msg %s: invalid website_id %q – skipping", msg.ID, websiteIDStr)
		return err
	}

	regionID, err := uuid.Parse(regionIDStr)
	if err != nil {
		log.Printf("msg %s: invalid region_id %q – skipping", msg.ID, regionIDStr)
		return err
	}

	if statusStr == "" {
		log.Printf("msg %s: missing status – skipping", msg.ID)
		return errors.New("missing status")
	}
	status := store.WebsiteStatus(statusStr)

	latency64, err := strconv.ParseInt(latencyStr, 10, 32)
	if err != nil {
		log.Printf("msg %s: invalid response_time_ms %q – skipping", msg.ID, latencyStr)
		return err
	}
	latencyMs := int32(latency64)

	if err := q.InsertWebsiteTick(ctx, store.InsertWebsiteTickParams{
		ID:             ID,
		WebsiteID:      websiteID,
		RegionID:       regionID,
		Status:         status,
		ResponseTimeMs: latencyMs,
	}); err != nil {
		log.Printf("msg %s: db insert failed: %v – will retry", msg.ID, err)
		return err
	}

	log.Printf("msg %s: persisted website_id=%s region_id=%s status=%s latency=%dms",
		msg.ID, websiteID, regionID, status, latencyMs)

	return nil
}

func isBusyGroup(err error) bool {
	return strings.Contains(err.Error(), "BUSYGROUP")
}
