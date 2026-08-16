// Command migrate applies the embedded goose migrations.
//
// Usage: PG_DSN=postgres://... migrate [up|down|status]   (default: up)
package main

import (
	"context"
	"database/sql"
	"log"
	"os"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib" // registers the "pgx" database/sql driver

	"example.com/agentmem/internal/store"
)

func main() {
	dsn := os.Getenv("PG_DSN")
	if dsn == "" {
		log.Fatal("migrate: PG_DSN environment variable is required")
	}
	command := "up"
	if len(os.Args) > 1 {
		command = os.Args[1]
	}

	ctx := context.Background()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		log.Fatalf("migrate: open: %v", err)
	}
	defer db.Close()

	// Postgres may still be starting when compose launches us; wait briefly.
	if err := waitForDB(ctx, db, 30*time.Second); err != nil {
		log.Fatalf("migrate: database not reachable: %v", err)
	}

	if err := store.RunGoose(ctx, db, command); err != nil {
		log.Fatalf("migrate: %s: %v", command, err)
	}
	log.Printf("migrate: %s complete", command)
}

func waitForDB(ctx context.Context, db *sql.DB, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	var err error
	for {
		pingCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		err = db.PingContext(pingCtx)
		cancel()
		if err == nil || time.Now().After(deadline) {
			return err
		}
		time.Sleep(500 * time.Millisecond)
	}
}
