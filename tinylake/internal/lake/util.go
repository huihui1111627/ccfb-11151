package lake

import (
	"encoding/json"
	"os"
	"time"
)

func nowMS() int64 { return time.Now().UnixMilli() }

func osPid() int { return os.Getpid() }

func timeAfter(d time.Duration) <-chan time.Time { return time.After(d) }

func osStat(name string) (os.FileInfo, error) { return os.Stat(name) }

func jsonMarshalBytes(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err) // all our types marshal cleanly
	}
	return b
}

func jsonUnmarshal(b []byte, v any) error { return json.Unmarshal(b, v) }
