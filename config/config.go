// Package config provides shared configuration types for the fb-tunnel library.
// Configuration is loaded from TOML files by both the client and server.
//
// Client configuration (client.toml):
//
//	firebase_url       = "https://YOUR-PROJECT-default-rtdb.firebaseio.com"
//	firebase_secret    = "YOUR_DATABASE_SECRET"
//	listen             = "127.0.0.1:1080"
//	batch_interval_ms  = 50
//	batch_max_bytes    = 32768
//	retry_limit        = 5
//
// Server configuration (server.toml):
//
//	firebase_url       = "https://YOUR-PROJECT-default-rtdb.firebaseio.com"
//	firebase_secret    = "YOUR_DATABASE_SECRET"
//	poll_interval_ms   = 200
//	session_timeout_s  = 300
//	retry_limit        = 5
package config

import (
	"fmt"
	"os"

	"github.com/BurntSushi/toml"
)

// ClientConfig holds configuration for the client binary.
type ClientConfig struct {
	// FirebaseURL is the full Firebase Realtime Database URL, e.g.
	// "https://my-project-default-rtdb.firebaseio.com".
	// Must NOT end with a trailing slash.
	FirebaseURL string `toml:"firebase_url"`

	// FirebaseSecret is the Firebase Database Secret (legacy auth token).
	FirebaseSecret string `toml:"firebase_secret"`

	// Listen is the local address + port to bind the SOCKS5 listener on.
	// Defaults to "127.0.0.1:1080".
	Listen string `toml:"listen"`

	// BatchIntervalMS is how many milliseconds to accumulate data before flushing a batch.
	// Lower values reduce latency; higher values reduce the number of Firebase writes.
	// Default: 50 ms.
	BatchIntervalMS uint64 `toml:"batch_interval_ms"`

	// BatchMaxBytes is the maximum number of bytes in a batch before it is flushed early
	// (regardless of the timer). Default: 32768 bytes (32 KiB).
	BatchMaxBytes int `toml:"batch_max_bytes"`

	// RetryLimit is how many times to retry a failed Firebase write before giving up.
	// Default: 5.
	RetryLimit uint32 `toml:"retry_limit"`
}

// ApplyDefaults fills in default values for any zero-value fields.
func (c *ClientConfig) ApplyDefaults() {
	if c.Listen == "" {
		c.Listen = "127.0.0.1:1080"
	}
	if c.BatchIntervalMS == 0 {
		c.BatchIntervalMS = 50
	}
	if c.BatchMaxBytes == 0 {
		c.BatchMaxBytes = 32 * 1024
	}
	if c.RetryLimit == 0 {
		c.RetryLimit = 5
	}
}

// ServerConfig holds configuration for the server binary.
type ServerConfig struct {
	// FirebaseURL is the full Firebase Realtime Database URL (same format as ClientConfig).
	FirebaseURL string `toml:"firebase_url"`

	// FirebaseSecret is the Firebase Database Secret.
	FirebaseSecret string `toml:"firebase_secret"`

	// PollIntervalMS is the interval in milliseconds between polls of the Firebase sessions list.
	// Lower values detect new sessions faster at the cost of more REST calls.
	// Default: 200 ms.
	PollIntervalMS uint64 `toml:"poll_interval_ms"`

	// SessionTimeoutS is the seconds of inactivity after which a session is considered dead
	// and is cleaned up. Default: 300 seconds (5 minutes).
	SessionTimeoutS uint64 `toml:"session_timeout_s"`

	// RetryLimit is how many times to retry a failed Firebase write. Default: 5.
	RetryLimit uint32 `toml:"retry_limit"`
}

// ApplyDefaults fills in default values for any zero-value fields.
func (c *ServerConfig) ApplyDefaults() {
	if c.PollIntervalMS == 0 {
		c.PollIntervalMS = 200
	}
	if c.SessionTimeoutS == 0 {
		c.SessionTimeoutS = 300
	}
	if c.RetryLimit == 0 {
		c.RetryLimit = 5
	}
}

// LoadClientConfig reads and parses a TOML client configuration file at path.
func LoadClientConfig(path string) (*ClientConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("cannot read config file %s: %w", path, err)
	}
	var cfg ClientConfig
	if _, err := toml.Decode(string(data), &cfg); err != nil {
		return nil, fmt.Errorf("cannot parse config file %s: %w", path, err)
	}
	cfg.ApplyDefaults()
	return &cfg, nil
}

// LoadServerConfig reads and parses a TOML server configuration file at path.
func LoadServerConfig(path string) (*ServerConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("cannot read config file %s: %w", path, err)
	}
	var cfg ServerConfig
	if _, err := toml.Decode(string(data), &cfg); err != nil {
		return nil, fmt.Errorf("cannot parse config file %s: %w", path, err)
	}
	cfg.ApplyDefaults()
	return &cfg, nil
}
