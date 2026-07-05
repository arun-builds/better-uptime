package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/arun-builds/better-uptime/internal/db"
	"github.com/arun-builds/better-uptime/internal/store"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/joho/godotenv"
	"github.com/redis/go-redis/v9"
)

const WebsiteChecksStream = "website-checks"

type Scheduler struct {
	queries *store.Queries
	redis   *redis.Client
	regions []store.Region
}

func main() {
	err := godotenv.Load()
	if err != nil {
		log.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pool, err := db.NewPool(ctx, 1, 2)
	if err != nil {
		log.Fatal(err)
	}
	defer pool.Close()

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

	queries := store.New(pool)

	regions, err := queries.ListRegions(ctx)
	if err != nil {
		log.Fatal(err)
	}

	s := &Scheduler{
		queries: queries,
		redis:   rdb,
		regions: regions,
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigCh
		log.Println("shutting down scheduler...")
		cancel()
	}()

	s.Run(ctx)
}

func (s *Scheduler) Run(ctx context.Context) {
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			s.schedule(ctx)
		case <-ctx.Done():
			log.Println("scheduler stopped")
			return
		}
	}
}

func (s *Scheduler) schedule(ctx context.Context) {
	log.Println("scheduler cycle starting")

	cursorTime := pgtype.Timestamp{Time: time.Time{}, Valid: true}
	cursorID := uuid.Nil
	const batchSize int32 = 100

	var totalJobs int64

	for {
		websites, err := s.queries.ListWebsitesBatch(ctx, store.ListWebsitesBatchParams{
			CursorCreatedAt: cursorTime,
			CursorID:        cursorID,
			BatchSize:       batchSize,
		})
		if err != nil {
			log.Printf("error fetching websites batch: %v", err)
			return
		}
		if len(websites) == 0 {
			break
		}

		log.Printf("loaded %d websites", len(websites))

		pipe := s.redis.Pipeline()
		for _, website := range websites {
			for _, region := range s.regions {
				stream := fmt.Sprintf("%s:%s", WebsiteChecksStream, region.CountryCode)
				pipe.XAdd(ctx, &redis.XAddArgs{
					Stream: stream,
					Values: map[string]any{
						"website_id": website.ID.String(),
						"url":        website.Url,
						"region_id":  region.ID.String(),
					},
				})
			}
		}

		_, err = pipe.Exec(ctx)
		if err != nil {
			log.Printf("error executing redis pipeline: %v", err)
		}

		totalJobs += int64(len(websites) * len(s.regions))

		last := websites[len(websites)-1]
		cursorTime = last.CreatedAt
		cursorID = last.ID
	}

	log.Printf("scheduler cycle complete, enqueued %d jobs", totalJobs)
}
