package main

import (
	"context"
	"net"
	"net/http"
	"strconv"
	"time"

	"github.com/arun-builds/better-uptime/internal/store"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

type CheckResult struct {
	ID             uuid.UUID
	WebsiteID      uuid.UUID
	RegionID       uuid.UUID
	Status         store.WebsiteStatus
	ResponseTimeMs int32
	CheckedAt      time.Time
}

var httpClient = &http.Client{
	Timeout: 10 * time.Second,
	CheckRedirect: func(req *http.Request, via []*http.Request) error {
		return http.ErrUseLastResponse
	},
}

func checkWebsite(id, websiteID, regionID uuid.UUID, url string) CheckResult {
	checkedAt := time.Now().UTC()

	start := time.Now()
	resp, err := httpClient.Get(url)
	latencyMs := int32(time.Since(start).Milliseconds())

	if err != nil {
		status := store.WebsiteStatusDOWN
		if nerr, ok := err.(net.Error); ok && nerr.Timeout() {
			status = store.WebsiteStatusTIMEOUT
		}
		return CheckResult{
			ID:             id,
			WebsiteID:      websiteID,
			RegionID:       regionID,
			Status:         status,
			ResponseTimeMs: latencyMs,
			CheckedAt:      checkedAt,
		}
	}
	defer resp.Body.Close()

	status := store.WebsiteStatusUP
	if resp.StatusCode >= http.StatusInternalServerError {
		status = store.WebsiteStatusDOWN
	}

	return CheckResult{

		WebsiteID:      websiteID,
		RegionID:       regionID,
		Status:         status,
		ResponseTimeMs: latencyMs,
		CheckedAt:      checkedAt,
	}
}

func publishResult(ctx context.Context, rdb *redis.Client, r CheckResult) error {
	return rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: ResultsStream,
		Values: map[string]any{
			"id":               r.ID.String(),
			"website_id":       r.WebsiteID.String(),
			"region_id":        r.RegionID.String(),
			"status":           string(r.Status),
			"response_time_ms": strconv.Itoa(int(r.ResponseTimeMs)),
			"checked_at":       r.CheckedAt.Format(time.RFC3339),
		},
	}).Err()
}
