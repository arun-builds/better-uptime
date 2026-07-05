package main

import (
	"net"
	"net/http"
	"time"

	"github.com/arun-builds/better-uptime/internal/store"
	"github.com/google/uuid"
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
