// Vulnerable web app with arbitrary file read for testing purposes
// DO NOT USE IN PRODUCTION - This is intentionally vulnerable
package main

import (
	"fmt"
	"io"
	"net/http"
	"os"
)

func main() {
	http.HandleFunc("/read", func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Query().Get("path")
		if path == "" {
			http.Error(w, "missing path parameter", http.StatusBadRequest)
			return
		}

		// VULNERABLE: No path validation - arbitrary file read
		f, err := os.Open(path)
		if err != nil {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}
		defer f.Close()

		w.Header().Set("Content-Type", "application/octet-stream")
		io.Copy(w, f)
	})

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}
	fmt.Printf("Vulnerable app listening on :%s\n", port)
	http.ListenAndServe(":"+port, nil)
}
