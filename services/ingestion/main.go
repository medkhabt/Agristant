package main 

import(
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"sync"
	"time"
	"path/filepath"
    "context"
	"sync/atomic"

    "github.com/redis/go-redis/v9"
)



type Event struct {
	Metric string            `json:"metric"`
	Value  float64           `json:"value"`
	TS     string            `json:"ts,omitempty"`   // RFC3339; optional in input
	Tags   map[string]string `json:"tags,omitempty"` // optional
}

var (
	mu sync.Mutex
	dataDir string 
)

func home(w http.ResponseWriter, r *http.Request){
	fmt.Fprintf(w, "Test api home");
	fmt.Println("Endpoint hit")

}

func reading(w http.ResponseWriter, r *http.Request) {
	atomic.AddUint64(&reqTotal, 1)

	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		atomic.AddUint64(&reqErrors, 1)
		return
	}

	r.Body = http.MaxBytesReader(w, r.Body, 1<<20) // 1 MB cap
	defer r.Body.Close()

	var in Event
	if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
		http.Error(w, "bad json", http.StatusBadRequest)
		atomic.AddUint64(&reqErrors, 1)
		return
	}
	if in.Metric == "" {
		http.Error(w, "metric required", http.StatusBadRequest)
		atomic.AddUint64(&reqErrors, 1)
		return
	}

	// Normalize timestamp to RFC3339 UTC
	now := time.Now().UTC()
	if in.TS == "" {
		in.TS = now.Format(time.RFC3339Nano)
	} else {
		if t, err := time.Parse(time.RFC3339, in.TS); err == nil {
			in.TS = t.UTC().Format(time.RFC3339Nano)
		} else {
			http.Error(w, "ts must be RFC3339", http.StatusBadRequest)
			atomic.AddUint64(&reqErrors, 1)
			return
		}
	}

	// Append as JSONL to daily file
	line, _ := json.Marshal(in)
	fn := filepath.Join(dataDir, fmt.Sprintf("readings-%s.jsonl", now.Format("20060102")))
	//fn := fmt.Sprintf("readings-%s.jsonl", now.Format("20060102"))

	mu.Lock()
	f, err := os.OpenFile(fn, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0777)
	if err == nil {
		_, err = f.Write(append(line, '\n'))
		_ = f.Close()
	}
	mu.Unlock()

	if err != nil {
		log.Printf("append error: %v", err)
		http.Error(w, "storage error", http.StatusInternalServerError)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	_, err = rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: "readings",
		Values: map[string]interface{}{"event": string(line)},
	}).Result()
	cancel()
	if err != nil {
		atomic.AddUint64(&redisErrs, 1)
		log.Printf("redis publish error: %v", err)
	}

	w.WriteHeader(http.StatusNoContent)


}
func handleRequests(){

	mux := http.NewServeMux()
	mux.HandleFunc("/", home)
	mux.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"status":"ok"}`))
	})

	mux.HandleFunc("/reading", reading)
	mux.Handle("/ready", readyHandler(dataDir))
	mux.HandleFunc("/metrics", metricsHandler)

	handler := reqLogger(mux)
	logJSON(logLine{Level:"info", Msg:"listening", Err:"", Path:":8080"})
	http.ListenAndServe(":8080", handler)

}
func main() {
	dataDir = getenv("DATA_DIR", "/data")
	redisAddr := getenv("REDIS_ADDR", "redis:6379")
	initRedis(redisAddr);

	
	handleRequests();

}

func getenv(k, def string) string {
	if v := os.Getenv(k); v != "" { return v }
	return def
}
