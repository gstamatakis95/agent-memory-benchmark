//go:build integration

package e2e

import "go.temporal.io/sdk/activity"

var (
	registerEcho = activity.RegisterOptions{Name: "echo"}
	registerFail = activity.RegisterOptions{Name: "fail"}
)
