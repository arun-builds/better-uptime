package main

import (
	"context"
	"log"
	"time"

	"github.com/arun-builds/better-uptime/internal/db"
	"github.com/arun-builds/better-uptime/internal/store"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/joho/godotenv"
	"github.com/redis/go-redis/v9"
)

const WebsiteChecksStream = "website-checks"

func main() {
	err := godotenv.Load()
	if err != nil {
		log.Fatal(err)
	}

	ctx := context.Background()
	pool, err := db.NewPool(ctx, 1, 2)
	if err != nil {
		log.Fatal(err)
	}
	defer pool.Close()

	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password
		DB:       0,  // use default DB
		Protocol: 2,
	})

	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Fatal(err)
	}

	// might be handy in case of connection string
	// opt, err := redis.ParseURL("redis://<user>:<pass>@localhost:6379/<db>")
	// if err != nil {
	// 	panic(err)
	// }

	// client := redis.NewClient(opt)
	//

	queries := store.New(pool)

	cursorTime := pgtype.Timestamp{
		Time:  time.Time{},
		Valid: true,
	}
	cursorID := uuid.Nil
	const batchSize int32 = 100

	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()

	for {

		cursorTime = pgtype.Timestamp{
			Time:  time.Time{},
			Valid: true,
		}
		cursorID = uuid.Nil

		websites, err := queries.ListWebsitesBatch(ctx, store.ListWebsitesBatchParams{
			CursorCreatedAt: cursorTime,
			CursorID:        cursorID,
			BatchSize:       batchSize,
		})
		if err != nil {
			log.Panic(err)
		}
		if len(websites) == 0 {
			break
		}

		log.Println("database: ", websites)

		pipe := rdb.Pipeline()

		for _, website := range websites {
			pipe.XAdd(ctx, &redis.XAddArgs{
				Stream: WebsiteChecksStream,
				Values: map[string]any{
					"website_id": website.ID.String(),
					"url":        website.Url,
				},
			})
		}

		_, err = pipe.Exec(ctx)
		if err != nil {
			log.Panic(err)
		}

		log.Println("pushed to reids")

		last := websites[len(websites)-1]
		cursorTime = last.CreatedAt
		cursorID = last.ID

		<-ticker.C

	}

}
