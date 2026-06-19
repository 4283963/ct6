package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"

	"ct6/internal/api"
	"ct6/internal/api/handler"   
	"ct6/internal/config"
	"ct6/internal/dispatcher"
	"ct6/internal/lock"
	"ct6/internal/metrics"
	"ct6/internal/middleware"
	"ct6/internal/repository"
	"ct6/internal/scheduler"
	"ct6/pkg/logger"
)

func main() {
	configPath := flag.String("config", "configs/config.yaml", "path to config file")
	migrate := flag.Bool("migrate", false, "auto migrate database schema before start")
	flag.Parse()

	cfg, err := config.Load(*configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "load config failed: %v\n", err)
		os.Exit(1)
	}

	logLevel := "info"
	if cfg.App.Environment == "prod" {
		logLevel = "info"
	} else {
		logLevel = "debug"
	}
	if err := logger.Init(logLevel, cfg.App.Environment); err != nil {
		fmt.Fprintf(os.Stderr, "init logger failed: %v\n", err)
		os.Exit(1)
	}
	defer logger.Sync()
	log := logger.L()

	log.Info("starting task-dispatcher",
		zap.String("env", cfg.App.Environment),
		zap.String("instance", cfg.App.InstanceID))

	// ---- 基础设施：MySQL / Redis ----
	db, err := repository.NewDB(cfg.MySQL)
	if err != nil {
		log.Fatal("connect mysql failed", zap.Error(err))
	}
	if *migrate {
		if err := repository.AutoMigrate(db); err != nil {
			log.Fatal("auto migrate failed", zap.Error(err))
		}
		log.Info("auto migrate completed")
	}

	rdb := redis.NewClient(&redis.Options{
		Addr:     cfg.Redis.Addr,
		Password: cfg.Redis.Password,
		DB:       cfg.Redis.DB,
		PoolSize: cfg.Redis.PoolSize,
	})
	pingCtx, pingCancel := context.WithTimeout(context.Background(), 5*time.Second)
	if err := rdb.Ping(pingCtx).Err(); err != nil {
		pingCancel()
		log.Fatal("connect redis failed", zap.Error(err))
	}
	pingCancel()

	// ---- 持久化层 ----
	taskRepo := repository.NewTaskRepository(db)
	execRepo := repository.NewExecutionRepository(db)

	// ---- 分布式锁 ----
	locker := lock.NewRedisLocker(rdb, cfg.Redis.LockNamespace)

	// ---- 实时计数器 (Redis) ----
	counters := metrics.NewDeliveryCounters(rdb, "td")

	// ---- 业务逻辑层：Dispatcher（先创建，Scheduler 依赖其 Submit） ----
	disp := dispatcher.NewDispatcher(taskRepo, execRepo, locker, cfg.Dispatcher, cfg.Scheduler, counters, cfg.App.InstanceID)

	// ---- 业务逻辑层：Scheduler ----
	sched := scheduler.NewScheduler(taskRepo, disp, cfg.Scheduler, cfg.Dispatcher, cfg.App.InstanceID)

	// ---- 根 context，统一驱动调度器/分发器生命周期 ----
	rootCtx, rootCancel := context.WithCancel(context.Background())
	defer rootCancel()

	go disp.Start(rootCtx)
	go sched.Start(rootCtx)

	// ---- 路由层 ----
	idemp := middleware.NewIdempotency(rdb, 24*time.Hour)
	taskH := handler.NewTaskHandler(sched, taskRepo, execRepo)
	statsSvc := metrics.NewStatsService(counters, execRepo, taskRepo, disp)
	statsH := handler.NewStatsHandler(statsSvc)
	healthH := handler.NewHealthHandler(db, rdb)
	engine := api.NewRouter(cfg, taskH, statsH, healthH, idemp)

	srv := &http.Server{
		Addr:         fmt.Sprintf(":%d", cfg.HTTP.Port),
		Handler:      engine,
		ReadTimeout:  cfg.HTTP.ReadTimeout,
		WriteTimeout: cfg.HTTP.WriteTimeout,
	}

	// ---- HTTP server + graceful shutdown ----
	serverErr := make(chan error, 1)
	go func() {
		log.Info("http server listening", zap.String("addr", srv.Addr))
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serverErr <- err
		}
		close(serverErr)
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	select {
	case err := <-serverErr:
		if err != nil {
			log.Error("http server error", zap.Error(err))
		}
	case sig := <-sigCh:
		log.Info("received signal, shutting down", zap.String("signal", sig.String()))
	}

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), cfg.HTTP.ShutdownTimeout)
	defer shutdownCancel()

	// 先停 HTTP（拒绝新请求），再停调度/分发。
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Error("http shutdown error", zap.Error(err))
	}
	disp.Stop()
	sched.Stop()
	rootCancel()

	// 等待 worker 排空队列（尽力而为）。
	time.Sleep(500 * time.Millisecond)
	sqlDB, _ := db.DB()
	if sqlDB != nil {
		_ = sqlDB.Close()
	}
	_ = rdb.Close()
	log.Info("shutdown complete")
}
