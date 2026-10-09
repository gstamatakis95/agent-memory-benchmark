//go:build integration

package e2e

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"go.temporal.io/api/workflowservice/v1"
)

// TestStack_Temporal checks what the smoke promises about Temporal (PLAN.md section 9.1): the frontend answers, the
// namespace `engram` exists with 7 days of retention, the cluster was created with 512 history shards, and the four
// services of the split deployment each registered in the membership ring.
func TestStack_Temporal(t *testing.T) {
	addr, _, pgDSN := need(t)
	c := dial(t, addr, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if _, err := c.CheckHealth(ctx, nil); err != nil {
		t.Fatalf("frontend health: %v", err)
	}
	ns, err := c.WorkflowService().DescribeNamespace(ctx,
		&workflowservice.DescribeNamespaceRequest{Namespace: "engram"})
	if err != nil {
		t.Fatal(err)
	}
	if got := ns.GetConfig().GetWorkflowExecutionRetentionTtl().AsDuration(); got != 7*24*time.Hour {
		t.Errorf("namespace engram retention = %v, want 168h", got)
	}
	conn, err := pgx.Connect(ctx, pgDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close(ctx) }()
	var shards int
	if err := conn.QueryRow(ctx, `SELECT count(*) FROM shards`).Scan(&shards); err != nil {
		t.Fatal(err)
	}
	if shards != 512 {
		t.Errorf("temporal persistence has %d history shards, want 512 (numHistoryShards, N71)", shards)
	}
	var roles int
	err = conn.QueryRow(ctx, `SELECT count(DISTINCT role) FROM cluster_membership
		 WHERE record_expiry > now() AT TIME ZONE 'utc'`).Scan(&roles)
	if err != nil {
		t.Fatal(err)
	}
	if roles != 4 {
		t.Errorf("%d service roles in the Temporal membership, want 4 (frontend, history, matching, worker)", roles)
	}
}
