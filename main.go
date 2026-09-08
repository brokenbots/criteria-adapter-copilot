package main

import (
	"context"
	"fmt"
	"os"

	adapterhost "github.com/brokenbots/criteria-go-adapter-sdk/adapterhost"
)

func main() {
	adapter := &copilotAdapter{
		sessions: map[string]*sessionState{},
	}

	remoteCfg, err := parseRemoteEnv()
	if err != nil {
		fmt.Fprintf(os.Stderr, "remote: %v\n", err)
		os.Exit(1)
	}
	if remoteCfg != nil {
		if err := ServeRemote(context.Background(), adapter, remoteCfg); err != nil {
			fmt.Fprintf(os.Stderr, "remote: %v\n", err)
			os.Exit(1)
		}
		return
	}

	adapterhost.Serve(adapter)
}
