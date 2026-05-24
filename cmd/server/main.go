package main

import (
	"github.com/go-chi/chi/v5"

	"github.com/LeezyWannaFall/Go-Search-Trends/internal/consumer"
	"github.com/LeezyWannaFall/Go-Search-Trends/internal/handler"
	"github.com/LeezyWannaFall/Go-Search-Trends/internal/service"
	"log"
	"net/http"
	"context"
	"os"
	"os/signal"
	"syscall"
	"strings"
)

func main() {
	// service and hanlder
	s := service.New()
	h := handler.New(s)

	// router
	r := chi.NewRouter()
	r.Get("/top", h.GetTop)
	r.Post("/stoplist/{word}", h.AddWord)
	r.Delete("/stoplist/{word}", h.DeleteWord)
	r.Get("/stoplist", h.GetBlackList)

	// server
	go func() {
		if err := http.ListenAndServe(":8080", r); err != nil {
			log.Fatalf("failed to start server: %v", err)
		}
	}()

	// kafka
	brokers := os.Getenv("KAFKA_BROKERS")
	if brokers == "" {
		brokers = "localhost:9092"
	}

	c := consumer.New(strings.Split(brokers, ","), "search-events", s)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	c.Run(ctx)
	log.Println("shutdown complete")
}