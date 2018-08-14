//go:generate goagen -d gitlab.com/remp/pythia/cmd/pythia_segments/design

package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/goadesign/goa"
	"github.com/goadesign/goa/middleware"
	"github.com/jmoiron/sqlx"
	"github.com/joho/godotenv"
	"github.com/kelseyhightower/envconfig"
	_ "github.com/lib/pq"
	"github.com/pkg/errors"
	"gitlab.com/remp/pythia/cmd/pythia_segments/app"
	"gitlab.com/remp/pythia/cmd/pythia_segments/controller"
	"gitlab.com/remp/pythia/cmd/pythia_segments/model"
)

func main() {
	err := godotenv.Load()
	if err != nil {
		log.Fatalln(errors.Wrap(err, "unable to load .env file"))
	}
	var c Config
	if err := envconfig.Process("pythia_segments", &c); err != nil {
		log.Fatalln(errors.Wrap(err, "unable to process envconfig"))
	}

	stop := make(chan os.Signal, 3)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)

	ctx, cancelCtx := context.WithCancel(context.Background())

	service := goa.New("pythia_segments")

	service.Use(middleware.RequestID())
	if c.Debug {
		service.Use(middleware.LogRequest(true))
		service.Use(middleware.LogResponse())
	}
	service.Use(middleware.ErrorHandler(service, true))
	service.Use(middleware.Recover())

	app.MountSwaggerController(service, service.NewController("swagger"))

	// DB init

	postgresDB, err := sqlx.Connect("postgres", fmt.Sprintf(
		"postgres://%s:%s@%s/%s?sslmode=disable",
		c.PostgresUser,
		c.PostgresPasswd,
		c.PostgresAddr,
		c.PostgresDBName,
	))
	if err != nil {
		log.Fatalln(errors.Wrap(err, "unable to connect to PostgreSQL"))
	}

	segmentStorage := &model.SegmentDB{
		Postgres: postgresDB,
	}

	// server cancellation

	var wg sync.WaitGroup

	// caching

	ticker := time.NewTicker(time.Minute * 5)
	defer ticker.Stop()

	cacheBrowsers := func() {
		if err := segmentStorage.CacheBrowsers(time.Now()); err != nil {
			service.LogError("unable to cache browsers", "err", err)
		}
	}

	wg.Add(1)
	segmentStorage.CacheSegments()
	cacheBrowsers()

	go func() {
		defer wg.Done()
		service.LogInfo("starting caching")
		for {
			select {
			case <-ticker.C:
				segmentStorage.CacheSegments()
				cacheBrowsers()
			case <-ctx.Done():
				service.LogInfo("caching stopped")
				return
			}
		}
	}()

	// controllers init

	app.MountSegmentsController(service, controller.NewSegmentController(service, segmentStorage))

	// server init

	service.LogInfo("starting server", "bind", c.PythiaSegmentsAddr)
	srv := &http.Server{
		Addr:    c.PythiaSegmentsAddr,
		Handler: service.Mux,
	}

	wg.Add(1)
	go func() {
		if err := srv.ListenAndServe(); err != nil {
			if err != http.ErrServerClosed {
				service.LogError("startup", "err", err)
				stop <- syscall.SIGQUIT
			}
			wg.Done()
		}
	}()

	s := <-stop
	service.LogInfo("shutting down", "signal", s)
	srv.Shutdown(ctx)
	cancelCtx()
	wg.Wait()
	service.LogInfo("bye bye")
}
