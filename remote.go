package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	adapterhost "github.com/brokenbots/criteria-go-adapter-sdk/adapterhost"
)

const (
	remoteHostEnv    = "CRITERIA_REMOTE_HOST"
	remoteTokenEnv   = "CRITERIA_REMOTE_TOKEN"
	remoteDigestEnv  = "CRITERIA_REMOTE_DIGEST"
	remoteNameEnv    = "CRITERIA_ADAPTER_NAME"
	remoteVersionEnv = "CRITERIA_ADAPTER_VERSION"

	remoteTLSCertEnv = "CRITERIA_REMOTE_TLS_CERT"
	remoteTLSKeyEnv  = "CRITERIA_REMOTE_TLS_KEY"
	remoteTLSCAEnv   = "CRITERIA_REMOTE_CA"
)

// remoteEnv holds the parsed remote-serve environment.
type remoteEnv struct {
	host    string
	token   string
	digest  string
	name    string
	version string
	tls     *tlsEnv
}

// tlsEnv holds the filesystem paths for mutual-TLS configuration.
type tlsEnv struct {
	certPath string
	keyPath  string
	caPath   string
}

// parseRemoteEnv reads remote-serve settings from the environment.
// It returns nil when CRITERIA_REMOTE_HOST is not set, indicating that
// local plugin mode should be used. A non-nil value means remote mode is
// requested; partially configured TLS paths are treated as an error.
func parseRemoteEnv() (*remoteEnv, error) {
	host := os.Getenv(remoteHostEnv)
	if host == "" {
		return nil, nil
	}

	env := &remoteEnv{
		host:    host,
		token:   os.Getenv(remoteTokenEnv),
		digest:  os.Getenv(remoteDigestEnv),
		name:    firstNonEmpty(os.Getenv(remoteNameEnv), adapterName),
		version: firstNonEmpty(os.Getenv(remoteVersionEnv), adapterVersion),
	}

	cert := os.Getenv(remoteTLSCertEnv)
	key := os.Getenv(remoteTLSKeyEnv)
	ca := os.Getenv(remoteTLSCAEnv)
	present := countNonEmpty(cert, key, ca)
	if present == 3 {
		env.tls = &tlsEnv{certPath: cert, keyPath: key, caPath: ca}
	} else if present > 0 {
		return nil, fmt.Errorf("remote: all or none of %s, %s, %s must be set; got %d of 3",
			remoteTLSCertEnv, remoteTLSKeyEnv, remoteTLSCAEnv, present)
	}

	return env, nil
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if v != "" {
			return v
		}
	}
	return ""
}

func countNonEmpty(values ...string) int {
	n := 0
	for _, v := range values {
		if v != "" {
			n++
		}
	}
	return n
}

// buildTLSConfig loads a mutual-TLS client configuration from the configured
// certificate paths. It returns nil when no TLS paths are configured.
func buildTLSConfig(cfg *tlsEnv) (*tls.Config, error) {
	if cfg == nil {
		return nil, nil
	}

	cert, err := tls.LoadX509KeyPair(cfg.certPath, cfg.keyPath)
	if err != nil {
		return nil, fmt.Errorf("remote: load client certificate: %w", err)
	}

	caData, err := os.ReadFile(cfg.caPath)
	if err != nil {
		return nil, fmt.Errorf("remote: read CA: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caData) {
		return nil, fmt.Errorf("remote: failed to parse CA from %s", cfg.caPath)
	}

	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		RootCAs:      pool,
		MinVersion:   tls.VersionTLS12,
	}, nil
}

// ServeRemote runs the phone-home remote adapter serve loop.
// It reconnects to the configured host with exponential backoff until the
// process receives SIGINT or SIGTERM.
func ServeRemote(ctx context.Context, adapter *copilotAdapter, cfg *remoteEnv) error {
	if cfg == nil {
		return errors.New("remote: configuration is nil")
	}

	tlsCfg, err := buildTLSConfig(cfg.tls)
	if err != nil {
		return err
	}

	opts := &adapterhost.ServeRemoteOptions{
		Host:        cfg.host,
		TLSConfig:   tlsCfg,
		Identity:    adapterhost.RemoteIdentity{Name: cfg.name, Version: cfg.version, Digest: cfg.digest},
		AcceptToken: cfg.token,
	}

	sigCtx, stop := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	backoff := []time.Duration{0, time.Second, 2 * time.Second, 5 * time.Second, 10 * time.Second}

	for attempt := 0; ; attempt++ {
		select {
		case <-sigCtx.Done():
			return nil
		default:
		}

		delay := backoff[min(attempt, len(backoff)-1)]
		timer := time.NewTimer(delay)
		select {
		case <-sigCtx.Done():
			timer.Stop()
			return nil
		case <-timer.C:
		}

		if err := adapterhost.ServeRemote(adapter, opts); err != nil {
			if sigCtx.Err() != nil {
				return nil
			}
			fmt.Fprintf(os.Stderr, "remote: connection closed (attempt %d): %v\n", attempt+1, err)
		}
	}
}
