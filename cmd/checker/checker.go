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

func checkWebsite(websiteID, regionID uuid.UUID, url string) CheckResult {
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

// publishResult writes a single check result event to the website-check-results
// Redis Stream.
//
// Event fields and why each is included:
//
//   website_id      – FK needed by Persistence (InsertWebsiteTick.WebsiteID).
//   region_id       – FK needed by Persistence (InsertWebsiteTick.RegionID).
//   status          – the probe outcome; needed by both Persistence and Alerting.
//   response_time_ms – latency in ms; needed by Persistence (InsertWebsiteTick.ResponseTimeMs).
//   checked_at      – precise probe timestamp (RFC3339 UTC). The DB uses NOW()
//                     on insert so this is the only record of when the probe
//                     actually ran; Alerting uses it to compute incident timing.
//
// Fields intentionally omitted:
//   url / name / user_id – derivable from the websites table via website_id.
func publishResult(ctx context.Context, rdb *redis.Client, r CheckResult) error {
	return rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: ResultsStream,
		Values: map[string]any{
			"website_id":       r.WebsiteID.String(),
			"region_id":        r.RegionID.String(),
			"status":           string(r.Status),
			"response_time_ms": strconv.Itoa(int(r.ResponseTimeMs)),
			"checked_at":       r.CheckedAt.Format(time.RFC3339),
		},
	}).Err()
}
