package main

import (
	"fmt"
	"os"

	adapterhost "github.com/brokenbots/criteria-go-adapter-sdk/adapterhost"
)

func main() {
	impl := newCopilotAdapter()
	if err := runAdapter(impl); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func newCopilotAdapter() *copilotAdapter {
	return &copilotAdapter{
		sessions: map[string]*sessionState{},
	}
}

func runAdapter(impl *copilotAdapter) error {
	host := os.Getenv("CRITERIA_REMOTE_HOST")
	if host == "" {
		adapterhost.Serve(impl)
		return nil
	}
	return serveRemote(impl)
}

func serveRemote(impl *copilotAdapter) error {
	opts, err := buildRemoteOptions()
	if err != nil {
		return err
	}
	return adapterhost.ServeRemote(impl, opts)
}

func buildRemoteOptions() (*adapterhost.ServeRemoteOptions, error) {
	opts := &adapterhost.ServeRemoteOptions{
		Host:        os.Getenv("CRITERIA_REMOTE_HOST"),
		AcceptToken: os.Getenv("CRITERIA_REMOTE_TOKEN"),
		Identity: adapterhost.RemoteIdentity{
			Name:    adapterName,
			Version: adapterVersion,
			Digest:  os.Getenv("CRITERIA_REMOTE_DIGEST"),
		},
		Reconnect: true,
	}

	cert := os.Getenv("CRITERIA_REMOTE_TLS_CERT")
	key := os.Getenv("CRITERIA_REMOTE_TLS_KEY")
	ca := os.Getenv("CRITERIA_REMOTE_CA")

	if cert != "" || key != "" || ca != "" {
		tlsConf, err := adapterhost.LoadClientTLS(cert, key, ca)
		if err != nil {
			return nil, fmt.Errorf("load remote TLS: %w", err)
		}
		opts.TLSConfig = tlsConf
	}

	return opts, nil
}
