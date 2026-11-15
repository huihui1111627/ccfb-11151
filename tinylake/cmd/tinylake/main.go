// Command tinylake starts the local storage-backed catalog and the management
// console HTTP server.
package main

import (
	"flag"
	"log"
	"net/http"

	"tinylake/internal/console"
	"tinylake/internal/lake"
)

func main() {
	root := flag.String("root", "./lake-data", "directory simulating the object-storage bucket")
	addr := flag.String("addr", ":8080", "HTTP listen address for the management console")
	flag.Parse()

	store, err := lake.NewLocalStorage(*root)
	if err != nil {
		log.Fatalf("init storage: %v", err)
	}
	cat := lake.NewCatalog(store)
	srv := console.NewServer(cat)

	log.Printf("TinyLake console listening on http://localhost%s (storage root: %s)", *addr, *root)
	if err := http.ListenAndServe(*addr, srv.Handler()); err != nil {
		log.Fatal(err)
	}
}
