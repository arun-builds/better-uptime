package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"

	"github.com/arun-builds/better-uptime/internal/db"
	"github.com/arun-builds/better-uptime/internal/store"
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/google/uuid"
	"github.com/joho/godotenv"
)

type CreateWebsiteRequest struct {
	Name string `json:"name"`
	URL  string `json:"url"`
}

type Handler struct {
	queries *store.Queries
}

func (h *Handler) CreateWebsiteHandler(w http.ResponseWriter, r *http.Request) {
	var req CreateWebsiteRequest

	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()

	if err := decoder.Decode(&req); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}

	if req.Name == "" || req.URL == "" {
		http.Error(w, "name and url are required", http.StatusBadRequest)
		return
	}
	// Save to database

	website, err := h.queries.CreateWebsite(
		r.Context(),
		store.CreateWebsiteParams{
			Name:   req.Name,
			Url:    req.URL,
			UserID: uuid.MustParse("5b3dfd3c-08b9-4b48-87a5-07912168d15a"),
		},
	)
	if err != nil {
		http.Error(w, "failed to create website", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(website)

}

func main() {

	err := godotenv.Load()
	if err != nil {
		log.Fatal(err)
	}

	ctx := context.Background()

	pool, err := db.NewPool(ctx)
	if err != nil {
		log.Fatal(err)
	}
	defer pool.Close()

	queries := store.New(pool)

	handler := &Handler{
		queries: queries,
	}

	r := chi.NewRouter()

	r.Use(middleware.Logger)

	r.Get("/", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("Hello World!"))
	})

	r.Post("/websites", handler.CreateWebsiteHandler)

	http.ListenAndServe(":8080", r)
}
