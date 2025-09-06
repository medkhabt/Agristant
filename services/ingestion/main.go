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
)

type Event struct {
	Metric string            `json:"metric"`
	Value  float64           `json:"value"`
	TS     string            `json:"ts,omitempty"`   // RFC3339; optional in input
	Tags   map[string]string `json:"tags,omitempty"` // optional
}

var mu sync.Mutex

func home(w http.ResponseWriter, r *http.Request){
	fmt.Fprintf(w, "Test api home");
	fmt.Println("Endpoint hit")

}

func reading(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		r.Body = http.MaxBytesReader(w, r.Body, 1<<20) // 1 MB cap
		defer r.Body.Close()

		var in Event
		if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
			http.Error(w, "bad json", http.StatusBadRequest)
			return
		}
		if in.Metric == "" {
			http.Error(w, "metric required", http.StatusBadRequest)
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
				return
			}
		}

		// Append as JSONL to daily file
		line, _ := json.Marshal(in)
		dir := os.Getenv("DATA_DIR")
		if dir == "" { dir = "." }
		fn := filepath.Join(dir, fmt.Sprintf("readings-%s.jsonl", now.Format("20060102")))
		//fn := fmt.Sprintf("readings-%s.jsonl", now.Format("20060102"))

		mu.Lock()
		f, err := os.OpenFile(fn, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
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

		w.WriteHeader(http.StatusNoContent)
	}
func handleRequests(){
	http.HandleFunc("/", home)
	http.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"status":"ok"}`))
	})

	http.HandleFunc("/reading", reading)

	log.Println("listening on :8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
		
}
func main() {
	handleRequests();

}
