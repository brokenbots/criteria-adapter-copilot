// main_test.go — tests for the adapter entrypoint's remote/local routing.

package main

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"

	adapterhost "github.com/brokenbots/criteria-go-adapter-sdk/adapterhost"
)

// generateTestClientCert creates a self-signed ECDSA certificate and key that
// can be loaded by adapterhost.LoadClientTLS. The same certificate is returned
// for both the client cert and the CA bundle so the test only exercises file
// loading, not TLS verification against a separate issuer.
func generateTestClientCert(t *testing.T) (certPEM, keyPEM []byte) {
	t.Helper()

	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}

	template := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "criteria-adapter-copilot-test"},
		NotBefore:    time.Now().Add(-time.Minute),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}

	certDER, err := x509.CreateCertificate(rand.Reader, &template, &template, &priv.PublicKey, priv)
	if err != nil {
		t.Fatalf("create certificate: %v", err)
	}

	certPEM = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})

	keyDER, err := x509.MarshalECPrivateKey(priv)
	if err != nil {
		t.Fatalf("marshal private key: %v", err)
	}
	keyPEM = pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})

	return certPEM, keyPEM
}

// TestBuildRemoteOptions_NoTLS verifies the remote options built from the
// standard CRITERIA_REMOTE_* environment variables when no TLS paths are set.
func TestBuildRemoteOptions_NoTLS(t *testing.T) {
	t.Setenv("CRITERIA_REMOTE_HOST", "criteria-host:7778")
	t.Setenv("CRITERIA_REMOTE_TOKEN", "remote-token")
	t.Setenv("CRITERIA_REMOTE_DIGEST", "sha256:abc123")
	t.Setenv("CRITERIA_REMOTE_TLS_CERT", "")
	t.Setenv("CRITERIA_REMOTE_TLS_KEY", "")
	t.Setenv("CRITERIA_REMOTE_CA", "")

	opts, err := buildRemoteOptions()
	if err != nil {
		t.Fatalf("buildRemoteOptions: %v", err)
	}
	if opts.Host != "criteria-host:7778" {
		t.Errorf("Host = %q, want criteria-host:7778", opts.Host)
	}
	if opts.AcceptToken != "remote-token" {
		t.Errorf("AcceptToken = %q, want remote-token", opts.AcceptToken)
	}
	if !opts.Reconnect {
		t.Error("Reconnect = false, want true")
	}
	if opts.TLSConfig != nil {
		t.Error("TLSConfig should be nil when no TLS paths are set")
	}
	if opts.Identity != (adapterhost.RemoteIdentity{Name: adapterName, Version: adapterVersion, Digest: "sha256:abc123"}) {
		t.Errorf("Identity = %+v, want name=%s version=%s digest=sha256:abc123", opts.Identity, adapterName, adapterVersion)
	}
}

// TestBuildRemoteOptions_WithTLS verifies that TLS paths are loaded when all
// three CRITERIA_REMOTE_TLS_* variables are provided. The files mounted by the
// Kubernetes sidecar are read by adapterhost.LoadClientTLS.
func TestBuildRemoteOptions_WithTLS(t *testing.T) {
	dir := t.TempDir()
	certPath := filepath.Join(dir, "tls.crt")
	keyPath := filepath.Join(dir, "tls.key")
	caPath := filepath.Join(dir, "ca.crt")

	certPEM, keyPEM := generateTestClientCert(t)

	if err := os.WriteFile(certPath, []byte(certPEM), 0o600); err != nil {
		t.Fatalf("write cert: %v", err)
	}
	if err := os.WriteFile(keyPath, []byte(keyPEM), 0o600); err != nil {
		t.Fatalf("write key: %v", err)
	}
	if err := os.WriteFile(caPath, []byte(certPEM), 0o600); err != nil {
		t.Fatalf("write ca: %v", err)
	}

	t.Setenv("CRITERIA_REMOTE_HOST", "criteria-host:7778")
	t.Setenv("CRITERIA_REMOTE_TOKEN", "remote-token")
	t.Setenv("CRITERIA_REMOTE_DIGEST", "sha256:abc123")
	t.Setenv("CRITERIA_REMOTE_TLS_CERT", certPath)
	t.Setenv("CRITERIA_REMOTE_TLS_KEY", keyPath)
	t.Setenv("CRITERIA_REMOTE_CA", caPath)

	opts, err := buildRemoteOptions()
	if err != nil {
		t.Fatalf("buildRemoteOptions: %v", err)
	}
	if opts.TLSConfig == nil {
		t.Fatal("TLSConfig should be set when TLS paths are provided")
	}
	if len(opts.TLSConfig.Certificates) != 1 {
		t.Errorf("len(TLSConfig.Certificates) = %d, want 1", len(opts.TLSConfig.Certificates))
	}
	if opts.TLSConfig.RootCAs == nil {
		t.Error("TLSConfig.RootCAs should be set")
	}
}

// TestBuildRemoteOptions_PartialTLS returns an error if only some TLS paths
// are provided, because a partial configuration is almost certainly a mount
// or manifest mistake.
func TestBuildRemoteOptions_PartialTLS(t *testing.T) {
	t.Setenv("CRITERIA_REMOTE_HOST", "criteria-host:7778")
	t.Setenv("CRITERIA_REMOTE_TLS_CERT", "/tmp/nonexistent-cert.pem")
	t.Setenv("CRITERIA_REMOTE_TLS_KEY", "")
	t.Setenv("CRITERIA_REMOTE_CA", "")

	if _, err := buildRemoteOptions(); err == nil {
		t.Fatal("expected error for partial TLS configuration")
	}
}

// TestBuildRemoteOptions_MissingKeyFile returns an error when the TLS key file
// does not exist.
func TestBuildRemoteOptions_MissingKeyFile(t *testing.T) {
	t.Setenv("CRITERIA_REMOTE_HOST", "criteria-host:7778")
	t.Setenv("CRITERIA_REMOTE_TLS_CERT", "/tmp/nonexistent-cert.pem")
	t.Setenv("CRITERIA_REMOTE_TLS_KEY", "/tmp/nonexistent-key.pem")
	t.Setenv("CRITERIA_REMOTE_CA", "/tmp/nonexistent-ca.pem")

	if _, err := buildRemoteOptions(); err == nil {
		t.Fatal("expected error for missing TLS key file")
	}
}
