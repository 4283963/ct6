package config

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

type Config struct {
	App        AppConfig        `yaml:"app"`
	HTTP       HTTPConfig       `yaml:"http"`
	MySQL      MySQLConfig      `yaml:"mysql"`
	Redis      RedisConfig      `yaml:"redis"`
	Scheduler  SchedulerConfig  `yaml:"scheduler"`
	Dispatcher DispatcherConfig `yaml:"dispatcher"`
}

type AppConfig struct {
	Name        string `yaml:"name"`
	Environment string `yaml:"environment"`
	InstanceID  string `yaml:"instance_id"`
}

type HTTPConfig struct {
	Port            int           `yaml:"port"`
	ReadTimeout     time.Duration `yaml:"read_timeout"`
	WriteTimeout    time.Duration `yaml:"write_timeout"`
	ShutdownTimeout time.Duration `yaml:"shutdown_timeout"`
}

type MySQLConfig struct {
	DSN             string        `yaml:"dsn"`
	MaxOpenConns    int           `yaml:"max_open_conns"`
	MaxIdleConns    int           `yaml:"max_idle_conns"`
	ConnMaxLifetime time.Duration `yaml:"conn_max_lifetime"`
}

type RedisConfig struct {
	Addr          string `yaml:"addr"`
	Password      string `yaml:"password"`
	DB            int    `yaml:"db"`
	PoolSize      int    `yaml:"pool_size"`
	LockNamespace string `yaml:"lock_namespace"`
}

type SchedulerConfig struct {
	TickInterval time.Duration `yaml:"tick_interval"`
	BatchSize    int           `yaml:"batch_size"`
	ClaimTTL     time.Duration `yaml:"claim_ttl"`
	MaxInFlight  int           `yaml:"max_in_flight"`
	WorkerCount  int           `yaml:"worker_count"`
}

type DispatcherConfig struct {
	HTTPTimeout       time.Duration `yaml:"http_timeout"`
	MaxRetries        int           `yaml:"max_retries"`
	BaseBackoff       time.Duration `yaml:"base_backoff"`
	MaxBackoff        time.Duration `yaml:"max_backoff"`
	BackoffMultiplier float64       `yaml:"backoff_multiplier"`
	JitterRatio       float64       `yaml:"jitter_ratio"`
	LockTTL           time.Duration `yaml:"lock_ttl"`
}

func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config file: %w", err)
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("unmarshal config: %w", err)
	}

	if err := cfg.validate(); err != nil {
		return nil, fmt.Errorf("validate config: %w", err)
	}
	cfg.applyDefaults()
	return &cfg, nil
}

func (c *Config) validate() error {
	if c.HTTP.Port == 0 {
		return fmt.Errorf("http.port is required")
	}
	if c.MySQL.DSN == "" {
		return fmt.Errorf("mysql.dsn is required")
	}
	if c.Redis.Addr == "" {
		return fmt.Errorf("redis.addr is required")
	}
	if c.Dispatcher.BackoffMultiplier <= 1 {
		return fmt.Errorf("dispatcher.backoff_multiplier must be > 1")
	}
	if c.Dispatcher.JitterRatio < 0 || c.Dispatcher.JitterRatio > 1 {
		return fmt.Errorf("dispatcher.jitter_ratio must be in [0,1]")
	}
	return nil
}

func (c *Config) applyDefaults() {
	if c.App.Name == "" {
		c.App.Name = "task-dispatcher"
	}
	if c.App.Environment == "" {
		c.App.Environment = "dev"
	}
	if c.App.InstanceID == "" {
		if host, err := os.Hostname(); err == nil {
			c.App.InstanceID = host
		} else {
			c.App.InstanceID = "instance-1"
		}
	}
	if c.HTTP.ReadTimeout == 0 {
		c.HTTP.ReadTimeout = 15 * time.Second
	}
	if c.HTTP.WriteTimeout == 0 {
		c.HTTP.WriteTimeout = 15 * time.Second
	}
	if c.HTTP.ShutdownTimeout == 0 {
		c.HTTP.ShutdownTimeout = 10 * time.Second
	}
	if c.MySQL.MaxOpenConns == 0 {
		c.MySQL.MaxOpenConns = 50
	}
	if c.MySQL.MaxIdleConns == 0 {
		c.MySQL.MaxIdleConns = 10
	}
	if c.MySQL.ConnMaxLifetime == 0 {
		c.MySQL.ConnMaxLifetime = time.Hour
	}
	if c.Redis.PoolSize == 0 {
		c.Redis.PoolSize = 200
	}
	if c.Redis.LockNamespace == "" {
		c.Redis.LockNamespace = "td:lock"
	}
	if c.Scheduler.TickInterval == 0 {
		c.Scheduler.TickInterval = 2 * time.Second
	}
	if c.Scheduler.BatchSize == 0 {
		c.Scheduler.BatchSize = 100
	}
	if c.Scheduler.ClaimTTL == 0 {
		c.Scheduler.ClaimTTL = 30 * time.Second
	}
	if c.Scheduler.MaxInFlight == 0 {
		c.Scheduler.MaxInFlight = 256
	}
	if c.Scheduler.WorkerCount == 0 {
		c.Scheduler.WorkerCount = 8
	}
	if c.Dispatcher.HTTPTimeout == 0 {
		c.Dispatcher.HTTPTimeout = 10 * time.Second
	}
	if c.Dispatcher.MaxRetries == 0 {
		c.Dispatcher.MaxRetries = 5
	}
	if c.Dispatcher.BaseBackoff == 0 {
		c.Dispatcher.BaseBackoff = time.Second
	}
	if c.Dispatcher.MaxBackoff == 0 {
		c.Dispatcher.MaxBackoff = 5 * time.Minute
	}
	if c.Dispatcher.BackoffMultiplier == 0 {
		c.Dispatcher.BackoffMultiplier = 2.0
	}
	if c.Dispatcher.LockTTL == 0 {
		c.Dispatcher.LockTTL = 30 * time.Second
	}
}
