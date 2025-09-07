package main

import (
	crypto "crypto/rand"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"time"
)

func rid() string {
	var b [16]byte
	_, _ = crypto.Read(b[:])
	return hex.EncodeToString(b[:])
}

type logLine struct {
	TS     string `json:"ts"`
	Level  string `json:"level"`
	Msg    string `json:"msg"`
	ReqID  string `json:"req_id,omitempty"`
	Method string `json:"method,omitempty"`
	Path   string `json:"path,omitempty"`
	Status int    `json:"status,omitempty"`
	DurMs  int64  `json:"dur_ms,omitempty"`
	Err    string `json:"err,omitempty"`
}

func logJSON(l logLine) {
	l.TS = time.Now().UTC().Format(time.RFC3339Nano)
	enc, _ := json.Marshal(l)
	println(string(enc))
}

func reqLogger(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := r.Header.Get("X-Request-ID")
		if id == "" { id = rid() }
		sw := &statusWriter{ResponseWriter: w, code: 200}
		start := time.Now()
		next.ServeHTTP(sw, r.WithContext(r.Context()))
		logJSON(logLine{
			Level:  "info",
			Msg:    "http",
			ReqID:  id,
			Method: r.Method,
			Path:   r.URL.Path,
			Status: sw.code,
			DurMs:  time.Since(start).Milliseconds(),
		})
	})
}

type statusWriter struct{ http.ResponseWriter; code int }
func (w *statusWriter) WriteHeader(c int){ w.code=c; w.ResponseWriter.WriteHeader(c) }
