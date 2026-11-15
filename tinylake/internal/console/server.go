// Package console serves the management HTTP API and the embedded single-page
// UI for inspecting and maintaining tinylake tables.
package console

import (
	"embed"
	"encoding/json"
	"net/http"
	"time"

	"tinylake/internal/lake"
)

//go:embed web/*
var webFS embed.FS

// Server wraps a catalog with HTTP handlers.
type Server struct {
	cat *lake.Catalog
	mux *http.ServeMux
}

func NewServer(cat *lake.Catalog) *Server {
	s := &Server{cat: cat, mux: http.NewServeMux()}
	s.routes()
	return s
}

func (s *Server) Handler() http.Handler {
	return logRequests(s.mux)
}

func (s *Server) routes() {
	s.mux.HandleFunc("GET /api/tables", s.listTables)
	s.mux.HandleFunc("POST /api/tables", s.createTable)
	s.mux.HandleFunc("GET /api/tables/{table}", s.tableDetail)
	s.mux.HandleFunc("GET /api/tables/{table}/history", s.history)
	s.mux.HandleFunc("GET /api/tables/{table}/files", s.files)
	s.mux.HandleFunc("POST /api/tables/{table}/compact", s.compact)
	s.mux.HandleFunc("POST /api/tables/{table}/rewrite", s.rewrite)
	s.mux.HandleFunc("POST /api/tables/{table}/expire", s.expire)
	s.mux.HandleFunc("POST /api/tables/{table}/spec", s.addSpec)
	s.mux.HandleFunc("GET /api/tables/{table}/conflicts", s.conflicts)

	// Static SPA.
	s.mux.Handle("GET /", http.FileServerFS(subFS(webFS, "web")))
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, code int, err error) {
	writeJSON(w, code, map[string]string{"error": err.Error()})
}

type tableSummary struct {
	Name        string             `json:"name"`
	Stat        lake.TableStat     `json:"stat"`
	CurrentSpec lake.PartitionSpec `json:"current_spec"`
}

func (s *Server) listTables(w http.ResponseWriter, r *http.Request) {
	names, err := s.cat.ListTables(r.Context())
	if err != nil {
		writeErr(w, 500, err)
		return
	}
	out := make([]tableSummary, 0, len(names))
	for _, n := range names {
		meta, err := s.cat.LoadTable(r.Context(), n)
		if err != nil {
			writeErr(w, 500, err)
			return
		}
		out = append(out, tableSummary{Name: n, Stat: meta.Stats(), CurrentSpec: meta.CurrentSpec()})
	}
	writeJSON(w, 200, out)
}

type createReq struct {
	Name          string   `json:"name"`
	PartitionCols []string `json:"partition_cols"`
}

func (s *Server) createTable(w http.ResponseWriter, r *http.Request) {
	var req createReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, 400, err)
		return
	}
	if req.Name == "" {
		writeJSON(w, 400, map[string]string{"error": "name required"})
		return
	}
	meta, err := s.cat.CreateTable(r.Context(), req.Name, req.PartitionCols)
	if err != nil {
		writeErr(w, 409, err)
		return
	}
	writeJSON(w, 201, meta.Stats())
}

func (s *Server) tableDetail(w http.ResponseWriter, r *http.Request) {
	meta, err := s.cat.LoadTable(r.Context(), r.PathValue("table"))
	if err != nil {
		writeErr(w, 404, err)
		return
	}
	writeJSON(w, 200, meta.Stats())
}

// historyEntry is one snapshot with derived display fields.
type historyEntry struct {
	lake.Snapshot
	AgeMS int64 `json:"age_ms"`
}

func (s *Server) history(w http.ResponseWriter, r *http.Request) {
	meta, err := s.cat.LoadTable(r.Context(), r.PathValue("table"))
	if err != nil {
		writeErr(w, 404, err)
		return
	}
	snaps := meta.Snapshots
	out := make([]historyEntry, 0, len(snaps))
	now := time.Now().UnixMilli()
	// newest first for the UI
	for i := len(snaps) - 1; i >= 0; i-- {
		out = append(out, historyEntry{Snapshot: snaps[i], AgeMS: now - snaps[i].CreatedMS})
	}
	writeJSON(w, 200, out)
}

type fileEntry struct {
	lake.DataFile
	AgeMS   int64 `json:"age_ms"`
	Deleted bool  `json:"deleted"`
}

func (s *Server) files(w http.ResponseWriter, r *http.Request) {
	meta, err := s.cat.LoadTable(r.Context(), r.PathValue("table"))
	if err != nil {
		writeErr(w, 404, err)
		return
	}
	dead := map[int64]bool{}
	for _, d := range meta.Deletes {
		dead[d.Key] = true
	}
	out := make([]fileEntry, 0, len(meta.Files))
	for _, f := range meta.Files {
		out = append(out, fileEntry{DataFile: f})
	}
	writeJSON(w, 200, map[string]any{
		"files":          out,
		"delete_markers": meta.Deletes,
		"stats":          meta.Stats(),
	})
}

type compactReq struct {
	TargetSize    int64 `json:"target_size"`
	MinInputFiles int   `json:"min_input_files"`
	MaxInputFiles int   `json:"max_input_files"`
}

func (s *Server) compact(w http.ResponseWriter, r *http.Request) {
	req := compactReq{TargetSize: 4096, MinInputFiles: 2, MaxInputFiles: 50}
	_ = json.NewDecoder(r.Body).Decode(&req) // body optional
	res, err := s.cat.Compact(r.Context(), r.PathValue("table"), lake.CompactionConfig{
		TargetSize: req.TargetSize, MinInputFiles: req.MinInputFiles, MaxInputFiles: req.MaxInputFiles,
	})
	if err != nil {
		writeErr(w, 409, err)
		return
	}
	writeJSON(w, 200, res)
}

type rewriteReq struct {
	MinMarkers int `json:"min_markers"`
}

func (s *Server) rewrite(w http.ResponseWriter, r *http.Request) {
	req := rewriteReq{MinMarkers: 1}
	_ = json.NewDecoder(r.Body).Decode(&req)
	res, err := s.cat.RewriteDeletes(r.Context(), r.PathValue("table"), req.MinMarkers)
	if err != nil {
		writeErr(w, 409, err)
		return
	}
	writeJSON(w, 200, res)
}

func (s *Server) expire(w http.ResponseWriter, r *http.Request) {
	res, err := s.cat.ExpireSnapshots(r.Context(), r.PathValue("table"))
	if err != nil {
		writeErr(w, 409, err)
		return
	}
	writeJSON(w, 200, res)
}

type specReq struct {
	Columns []string `json:"columns"`
}

func (s *Server) addSpec(w http.ResponseWriter, r *http.Request) {
	var req specReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, 400, err)
		return
	}
	id, err := s.cat.AddPartitionSpec(r.Context(), r.PathValue("table"), req.Columns)
	if err != nil {
		writeErr(w, 409, err)
		return
	}
	writeJSON(w, 200, map[string]int{"spec_id": id})
}

// conflictStats derives concurrency information from snapshot history: how
// many writers were active, CAS contention windows (snapshots sharing the
// same parent), and duplicate/idempotent commits.
func (s *Server) conflicts(w http.ResponseWriter, r *http.Request) {
	meta, err := s.cat.LoadTable(r.Context(), r.PathValue("table"))
	if err != nil {
		writeErr(w, 404, err)
		return
	}
	children := map[int64]int{}
	writers := map[string]int{}
	dupes := map[string]int{}
	for _, sn := range meta.Snapshots {
		children[sn.ParentID]++
		writers[sn.WriterID]++
		dupes[sn.CommitID]++
	}
	contendedParents := 0
	for _, n := range children {
		if n > 1 {
			contendedParents++
		}
	}
	duplicateCommits := 0
	for _, n := range dupes {
		if n > 1 {
			duplicateCommits++
		}
	}
	// Per-writer timeline for the conflict diagram.
	type writerRow struct {
		Writer  string `json:"writer"`
		Commits int    `json:"commits"`
	}
	rows := make([]writerRow, 0, len(writers))
	for w, n := range writers {
		rows = append(rows, writerRow{Writer: w, Commits: n})
	}
	writeJSON(w, 200, map[string]any{
		"total_snapshots":   len(meta.Snapshots),
		"distinct_writers":  len(writers),
		"contended_parents": contendedParents,
		"duplicate_commits": duplicateCommits,
		"writers":           rows,
		"snapshots":         meta.Snapshots,
	})
}
