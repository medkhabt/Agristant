package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
)

var (
	rdb        *redis.Client
	reqTotal   uint64
	reqErrors  uint64
	appendErrs uint64
	redisErrs  uint64
)

func initRedis(addr string) {
	rdb = redis.NewClient(&redis.Options{Addr: addr})
}

func checkWritable(dir string) error {
	fn := filepath.Join(dir, ".readycheck.tmp")
	f, err := os.OpenFile(fn, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil { return err }
	defer os.Remove(fn)
	_, err = f.Write([]byte("ok"))
	return err
}

func readyHandler(dataDir string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if err := checkWritable(dataDir); err != nil {
			http.Error(w, "data dir not writable", 503); return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 1*time.Second)
		defer cancel()
		if err := rdb.Ping(ctx).Err(); err != nil {
			http.Error(w, "redis not reachable", 503); return
		}
		w.Header().Set("Content-Type","application/json")
		w.Write([]byte(`{"ready":true}`))
	}
}

func metricsHandler(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	fmt.Fprintf(w, "ingestion_requests_total %d\n", atomic.LoadUint64(&reqTotal))
	fmt.Fprintf(w, "ingestion_request_errors_total %d\n", atomic.LoadUint64(&reqErrors))
	fmt.Fprintf(w, "ingestion_append_errors_total %d\n", atomic.LoadUint64(&appendErrs))
	fmt.Fprintf(w, "ingestion_redis_publish_errors_total %d\n", atomic.LoadUint64(&redisErrs))
}
