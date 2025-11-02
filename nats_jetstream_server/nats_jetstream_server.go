package nats_jetstream_server

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/spf13/viper"
	"go.uber.org/fx"
	"go.uber.org/zap"
)

const (
	DefaultHost                = "0.0.0.0"
	DefaultPort                = 4222
	DefaultHTTPPort            = 8222
	DefaultClusterPort         = 6222
	DefaultJetStreamEnabled    = true
	DefaultJetStreamMaxMemory  = 1024 * 1024 * 1024      // 1GB
	DefaultJetStreamMaxStorage = 10 * 1024 * 1024 * 1024 // 10GB
	DefaultMaxConnections      = 64 * 1024
	DefaultMaxPayload          = 1024 * 1024 // 1MB
	DefaultWriteDeadline       = "2s"
	DefaultStoreDir            = "./data/jetstream"
	DefaultLogLevel            = "INFO"
)

var logger *zap.Logger

type NATSJetStreamServer struct {
	logger *zap.Logger
	server *server.Server
	scope  string
	opts   *server.Options
}

type Params struct {
	fx.In

	Lifecycle fx.Lifecycle
	Logger    *zap.Logger
}

func Module(scope string) fx.Option {

	var s *NATSJetStreamServer

	return fx.Module(
		scope,
		fx.Provide(func(p Params) *NATSJetStreamServer {

			logger = p.Logger.Named(scope)

			s := &NATSJetStreamServer{
				logger: logger,
				scope:  scope,
			}

			s.initDefaultConfigs()

			return s
		}),
		fx.Populate(&s),
		fx.Invoke(func(p Params) {

			p.Lifecycle.Append(
				fx.Hook{
					OnStart: s.onStart,
					OnStop:  s.onStop,
				},
			)
		}),
	)
}

func (s *NATSJetStreamServer) getConfigPath(key string) string {
	return fmt.Sprintf("%s.%s", s.scope, key)
}

func (s *NATSJetStreamServer) initDefaultConfigs() {
	// Server basic configs
	viper.SetDefault(s.getConfigPath("host"), DefaultHost)
	viper.SetDefault(s.getConfigPath("port"), DefaultPort)
	viper.SetDefault(s.getConfigPath("http_port"), DefaultHTTPPort)
	viper.SetDefault(s.getConfigPath("cluster_port"), DefaultClusterPort)
	viper.SetDefault(s.getConfigPath("max_connections"), DefaultMaxConnections)
	viper.SetDefault(s.getConfigPath("max_payload"), DefaultMaxPayload)
	viper.SetDefault(s.getConfigPath("write_deadline"), DefaultWriteDeadline)
	viper.SetDefault(s.getConfigPath("log_level"), DefaultLogLevel)

	// JetStream configs
	viper.SetDefault(s.getConfigPath("jetstream.enabled"), DefaultJetStreamEnabled)
	viper.SetDefault(s.getConfigPath("jetstream.max_memory"), DefaultJetStreamMaxMemory)
	viper.SetDefault(s.getConfigPath("jetstream.max_storage"), DefaultJetStreamMaxStorage)
	viper.SetDefault(s.getConfigPath("jetstream.store_dir"), DefaultStoreDir)

	// Cluster configs
	viper.SetDefault(s.getConfigPath("cluster.enabled"), false)
	viper.SetDefault(s.getConfigPath("cluster.name"), "")
	viper.SetDefault(s.getConfigPath("cluster.routes"), []string{})

	// Auth configs
	viper.SetDefault(s.getConfigPath("auth.enabled"), false)
	viper.SetDefault(s.getConfigPath("auth.username"), "")
	viper.SetDefault(s.getConfigPath("auth.password"), "")
	viper.SetDefault(s.getConfigPath("auth.token"), "")

	// TLS configs
	viper.SetDefault(s.getConfigPath("tls.enabled"), false)
	viper.SetDefault(s.getConfigPath("tls.cert_file"), "")
	viper.SetDefault(s.getConfigPath("tls.key_file"), "")
	viper.SetDefault(s.getConfigPath("tls.ca_file"), "")
}

func (s *NATSJetStreamServer) buildServerOptions() (*server.Options, error) {
	opts := &server.Options{}

	// Basic server configuration
	host := viper.GetString(s.getConfigPath("host"))
	port := viper.GetInt(s.getConfigPath("port"))
	httpPort := viper.GetInt(s.getConfigPath("http_port"))

	opts.Host = host
	opts.Port = port
	opts.HTTPHost = host
	opts.HTTPPort = httpPort
	opts.MaxConn = viper.GetInt(s.getConfigPath("max_connections"))
	opts.MaxPayload = int32(viper.GetInt(s.getConfigPath("max_payload")))

	// Parse write deadline
	writeDeadlineStr := viper.GetString(s.getConfigPath("write_deadline"))
	if writeDeadline, err := time.ParseDuration(writeDeadlineStr); err == nil {
		opts.WriteDeadline = writeDeadline
	}

	// Logging configuration
	logLevel := viper.GetString(s.getConfigPath("log_level"))
	opts.Debug = logLevel == "DEBUG"
	opts.Trace = logLevel == "TRACE"
	opts.Logtime = true

	// JetStream configuration
	if viper.GetBool(s.getConfigPath("jetstream.enabled")) {
		storeDir := viper.GetString(s.getConfigPath("jetstream.store_dir"))

		// Create store directory if it doesn't exist
		if err := os.MkdirAll(storeDir, 0755); err != nil {
			return nil, fmt.Errorf("failed to create JetStream store directory: %w", err)
		}

		opts.JetStream = true
		opts.StoreDir = storeDir
		opts.JetStreamMaxMemory = int64(viper.GetInt(s.getConfigPath("jetstream.max_memory")))
		opts.JetStreamMaxStore = int64(viper.GetInt(s.getConfigPath("jetstream.max_storage")))
	}

	// Cluster configuration
	if viper.GetBool(s.getConfigPath("cluster.enabled")) {
		clusterPort := viper.GetInt(s.getConfigPath("cluster_port"))
		clusterName := viper.GetString(s.getConfigPath("cluster.name"))
		routes := viper.GetStringSlice(s.getConfigPath("cluster.routes"))

		opts.Cluster.Host = host
		opts.Cluster.Port = clusterPort
		opts.Cluster.Name = clusterName

		// Parse cluster routes
		for _, route := range routes {
			parsedURL, err := url.Parse(route)
			if err != nil {
				return nil, fmt.Errorf("invalid cluster route URL %s: %w", route, err)
			}
			opts.Routes = append(opts.Routes, parsedURL)
		}
	}

	// Authentication configuration
	if viper.GetBool(s.getConfigPath("auth.enabled")) {
		username := viper.GetString(s.getConfigPath("auth.username"))
		password := viper.GetString(s.getConfigPath("auth.password"))
		token := viper.GetString(s.getConfigPath("auth.token"))

		if token != "" {
			opts.Authorization = token
		} else if username != "" && password != "" {
			opts.Username = username
			opts.Password = password
		}
	}

	// TLS configuration
	if viper.GetBool(s.getConfigPath("tls.enabled")) {
		certFile := viper.GetString(s.getConfigPath("tls.cert_file"))
		keyFile := viper.GetString(s.getConfigPath("tls.key_file"))
		caFile := viper.GetString(s.getConfigPath("tls.ca_file"))

		if certFile == "" || keyFile == "" {
			return nil, fmt.Errorf("TLS enabled but cert_file or key_file not specified")
		}

		// Convert to absolute paths
		if !filepath.IsAbs(certFile) {
			certFile, _ = filepath.Abs(certFile)
		}
		if !filepath.IsAbs(keyFile) {
			keyFile, _ = filepath.Abs(keyFile)
		}

		// Load TLS certificates
		cert, err := tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			return nil, fmt.Errorf("failed to load TLS certificate: %w", err)
		}

		tlsConfig := &tls.Config{
			Certificates: []tls.Certificate{cert},
		}

		// Load CA certificate if specified
		if caFile != "" {
			if !filepath.IsAbs(caFile) {
				caFile, _ = filepath.Abs(caFile)
			}

			caCert, err := os.ReadFile(caFile)
			if err != nil {
				return nil, fmt.Errorf("failed to read CA certificate file: %w", err)
			}

			caCertPool := x509.NewCertPool()
			if !caCertPool.AppendCertsFromPEM(caCert) {
				return nil, fmt.Errorf("failed to parse CA certificate")
			}

			tlsConfig.ClientCAs = caCertPool
			tlsConfig.ClientAuth = tls.RequireAndVerifyClientCert
		}

		opts.TLSConfig = tlsConfig
		opts.TLS = true
	}

	return opts, nil
}

func (s *NATSJetStreamServer) onStart(ctx context.Context) error {
	var err error

	// Build server options
	s.opts, err = s.buildServerOptions()
	if err != nil {
		return fmt.Errorf("failed to build server options: %w", err)
	}

	// Disable signal handling to avoid conflicts in embedded mode
	s.opts.NoSigs = true

	logger.Info("Starting NATS JetStream Server",
		zap.String("host", s.opts.Host),
		zap.Int("port", s.opts.Port),
		zap.Int("http_port", s.opts.HTTPPort),
		zap.Bool("jetstream_enabled", s.opts.JetStream),
		zap.String("store_dir", s.opts.StoreDir),
		zap.Bool("cluster_enabled", s.opts.Cluster.Port > 0),
	)

	// Create and start the server
	s.server, err = server.NewServer(s.opts)
	if err != nil {
		return fmt.Errorf("failed to create NATS server: %w", err)
	}

	// Configure server logger to use our logger
	s.server.ConfigureLogger()

	// Start the server in a goroutine
	go s.server.Start()

	// Wait for server to be ready
	if !s.server.ReadyForConnections(10 * time.Second) {
		return fmt.Errorf("NATS server failed to start within timeout")
	}

	logger.Info("NATS JetStream Server started successfully",
		zap.String("client_url", fmt.Sprintf("nats://%s:%d", s.opts.Host, s.opts.Port)),
		zap.String("http_url", fmt.Sprintf("http://%s:%d", s.opts.HTTPHost, s.opts.HTTPPort)),
	)

	return nil
}

func (s *NATSJetStreamServer) onStop(ctx context.Context) error {
	if s.server != nil {
		logger.Info("Stopping NATS JetStream Server")

		// Graceful shutdown with timeout
		s.server.Shutdown()

		// Wait for shutdown to complete
		done := make(chan struct{})
		go func() {
			s.server.WaitForShutdown()
			close(done)
		}()

		select {
		case <-done:
			logger.Info("NATS JetStream Server stopped gracefully")
		case <-time.After(30 * time.Second):
			logger.Warn("NATS JetStream Server shutdown timeout, forcing stop")
		}
	}

	return nil
}

// GetServer returns the NATS server instance
func (s *NATSJetStreamServer) GetServer() *server.Server {
	return s.server
}

// GetClientURL returns the client connection URL
func (s *NATSJetStreamServer) GetClientURL() string {
	if s.opts == nil {
		return ""
	}
	return fmt.Sprintf("nats://%s:%d", s.opts.Host, s.opts.Port)
}

// GetHTTPURL returns the HTTP monitoring URL
func (s *NATSJetStreamServer) GetHTTPURL() string {
	if s.opts == nil {
		return ""
	}
	return fmt.Sprintf("http://%s:%d", s.opts.HTTPHost, s.opts.HTTPPort)
}

// IsRunning returns true if the server is running
func (s *NATSJetStreamServer) IsRunning() bool {
	if s.server == nil {
		return false
	}
	// Check if server is running by attempting to get server info
	// This is a workaround since IsRunning() is not exported
	return s.server.ReadyForConnections(0)
}

// GetConnectionCount returns the current number of connections
func (s *NATSJetStreamServer) GetConnectionCount() int {
	if s.server == nil {
		return 0
	}
	return s.server.NumClients()
}
