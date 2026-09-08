package main

import (
	"path/filepath"
	"testing"
)

func resetRemoteEnv(t *testing.T) {
	t.Helper()
	for _, k := range []string{
		remoteHostEnv,
		remoteTokenEnv,
		remoteDigestEnv,
		remoteNameEnv,
		remoteVersionEnv,
		remoteTLSCertEnv,
		remoteTLSKeyEnv,
		remoteTLSCAEnv,
	} {
		t.Setenv(k, "")
	}
}

func TestParseRemoteEnv_NoHost(t *testing.T) {
	resetRemoteEnv(t)
	cfg, err := parseRemoteEnv()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg != nil {
		t.Fatalf("expected nil config when %s is unset, got %+v", remoteHostEnv, cfg)
	}
}

func TestParseRemoteEnv_HostOnly(t *testing.T) {
	resetRemoteEnv(t)
	t.Setenv(remoteHostEnv, "example.com:7778")

	cfg, err := parseRemoteEnv()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg == nil {
		t.Fatal("expected non-nil config")
	}
	if cfg.host != "example.com:7778" {
		t.Errorf("host = %q, want example.com:7778", cfg.host)
	}
	if cfg.name != adapterName {
		t.Errorf("name = %q, want %q", cfg.name, adapterName)
	}
	if cfg.version != adapterVersion {
		t.Errorf("version = %q, want %q", cfg.version, adapterVersion)
	}
	if cfg.token != "" {
		t.Errorf("token = %q, want empty", cfg.token)
	}
	if cfg.digest != "" {
		t.Errorf("digest = %q, want empty", cfg.digest)
	}
	if cfg.tls != nil {
		t.Errorf("tls = %+v, want nil", cfg.tls)
	}
}

func TestParseRemoteEnv_AllFields(t *testing.T) {
	resetRemoteEnv(t)
	t.Setenv(remoteHostEnv, "host.example:7778")
	t.Setenv(remoteTokenEnv, "secret-token")
	t.Setenv(remoteDigestEnv, "sha256:abc123")
	t.Setenv(remoteNameEnv, "custom-adapter")
	t.Setenv(remoteVersionEnv, "2.0.0")

	cfg, err := parseRemoteEnv()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.host != "host.example:7778" {
		t.Errorf("host = %q", cfg.host)
	}
	if cfg.token != "secret-token" {
		t.Errorf("token = %q", cfg.token)
	}
	if cfg.digest != "sha256:abc123" {
		t.Errorf("digest = %q", cfg.digest)
	}
	if cfg.name != "custom-adapter" {
		t.Errorf("name = %q", cfg.name)
	}
	if cfg.version != "2.0.0" {
		t.Errorf("version = %q", cfg.version)
	}
}

func TestParseRemoteEnv_DefaultIdentity(t *testing.T) {
	resetRemoteEnv(t)
	t.Setenv(remoteHostEnv, "example.com:7778")

	cfg, err := parseRemoteEnv()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.name != adapterName {
		t.Errorf("name default = %q, want %q", cfg.name, adapterName)
	}
	if cfg.version != adapterVersion {
		t.Errorf("version default = %q, want %q", cfg.version, adapterVersion)
	}
}

func TestParseRemoteEnv_TLSPartial(t *testing.T) {
	resetRemoteEnv(t)
	t.Setenv(remoteHostEnv, "example.com:7778")
	t.Setenv(remoteTLSCertEnv, "/certs/cert.pem")

	cfg, err := parseRemoteEnv()
	if err == nil {
		t.Fatal("expected error for partial TLS configuration")
	}
	if cfg != nil {
		t.Fatalf("expected nil config on error, got %+v", cfg)
	}
}

func TestParseRemoteEnv_TLSComplete(t *testing.T) {
	resetRemoteEnv(t)
	t.Setenv(remoteHostEnv, "example.com:7778")
	t.Setenv(remoteTLSCertEnv, "/certs/cert.pem")
	t.Setenv(remoteTLSKeyEnv, "/certs/key.pem")
	t.Setenv(remoteTLSCAEnv, "/certs/ca.pem")

	cfg, err := parseRemoteEnv()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg == nil {
		t.Fatal("expected non-nil config")
	}
	if cfg.tls == nil {
		t.Fatal("expected TLS config")
	}
	if cfg.tls.certPath != "/certs/cert.pem" {
		t.Errorf("certPath = %q", cfg.tls.certPath)
	}
	if cfg.tls.keyPath != "/certs/key.pem" {
		t.Errorf("keyPath = %q", cfg.tls.keyPath)
	}
	if cfg.tls.caPath != "/certs/ca.pem" {
		t.Errorf("caPath = %q", cfg.tls.caPath)
	}
}

func TestBuildTLSConfig_NoConfig(t *testing.T) {
	cfg, err := buildTLSConfig(nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg != nil {
		t.Fatalf("expected nil tls.Config, got %+v", cfg)
	}
}

func TestBuildTLSConfig_Valid(t *testing.T) {
	base := "testfixtures/remote"
	tlsEnv := &tlsEnv{
		certPath: filepath.Join(base, "client-cert.pem"),
		keyPath:  filepath.Join(base, "client-key.pem"),
		caPath:   filepath.Join(base, "ca-cert.pem"),
	}

	cfg, err := buildTLSConfig(tlsEnv)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg == nil {
		t.Fatal("expected non-nil tls.Config")
	}
	if len(cfg.Certificates) != 1 {
		t.Errorf("certificates = %d, want 1", len(cfg.Certificates))
	}
	if cfg.RootCAs == nil {
		t.Error("expected RootCAs to be set")
	}
	if cfg.MinVersion == 0 {
		t.Error("expected MinVersion to be set")
	}
}

func TestBuildTLSConfig_MissingFiles(t *testing.T) {
	base := "testfixtures/remote"

	cases := []struct {
		name string
		cfg  *tlsEnv
	}{
		{
			name: "missing cert",
			cfg:  &tlsEnv{certPath: filepath.Join(base, "missing.pem"), keyPath: filepath.Join(base, "client-key.pem"), caPath: filepath.Join(base, "ca-cert.pem")},
		},
		{
			name: "missing key",
			cfg:  &tlsEnv{certPath: filepath.Join(base, "client-cert.pem"), keyPath: filepath.Join(base, "missing.pem"), caPath: filepath.Join(base, "ca-cert.pem")},
		},
		{
			name: "missing ca",
			cfg:  &tlsEnv{certPath: filepath.Join(base, "client-cert.pem"), keyPath: filepath.Join(base, "client-key.pem"), caPath: filepath.Join(base, "missing.pem")},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := buildTLSConfig(tc.cfg)
			if err == nil {
				t.Fatal("expected error for missing file")
			}
		})
	}
}

func TestBuildTLSConfig_InvalidCA(t *testing.T) {
	base := "testfixtures/remote"

	tlsEnv := &tlsEnv{
		certPath: filepath.Join(base, "client-cert.pem"),
		keyPath:  filepath.Join(base, "client-key.pem"),
		caPath:   filepath.Join(base, "invalid-ca.pem"),
	}

	_, err := buildTLSConfig(tlsEnv)
	if err == nil {
		t.Fatal("expected error for invalid CA")
	}
}

