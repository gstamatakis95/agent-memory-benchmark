package main

import "go.temporal.io/sdk/activity"

func registerOpts(name string) activity.RegisterOptions { return activity.RegisterOptions{Name: name} }
