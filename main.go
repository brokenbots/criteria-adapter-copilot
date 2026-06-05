package main

import (
	adapterhost "github.com/brokenbots/criteria-go-adapter-sdk/adapterhost"
)

func main() {
	adapterhost.Serve(&copilotAdapter{
		sessions: map[string]*sessionState{},
	})
}
