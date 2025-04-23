package metrics

// import (
// 	"context"
// 	"fmt"
// 	"net/http"
// 	"os"
// 	"os/signal"
// 	"time"
//
// 	"github.com/0xPolygonHermez/zkevm-bridge-service/log"
// 	"github.com/prometheus/client_golang/prometheus/promhttp"
// )
//
// // StartMetricsHttpServer initializes the metrics registry and starts the prometheus metrics HTTP server
// func StartMetricsHttpServer(c Config) {
// 	ctx, cancel := context.WithCancel(context.Background())
// 	defer cancel()
//
// 	if !c.Enabled {
// 		return
// 	}
//
// 	// Init metrics registry
// 	initMetrics(c)
//
// 	// Start metrics HTTP server
// 	mux := http.NewServeMux()
// 	addr := fmt.Sprintf(":%s", c.Port)
//
// 	endpoint := c.Endpoint
// 	if endpoint == "" {
// 		endpoint = defaultMetricsEndpoint
// 	}
//
// 	mux.Handle(endpoint, promhttp.Handler())
// 	srv := &http.Server{
// 		Addr:        addr,
// 		Handler:     mux,
// 		ReadTimeout: 5 * time.Second, //nolint:gomnd
// 	}
//
// 	ch := make(chan os.Signal, 1)
// 	signal.Notify(ch, os.Interrupt)
// 	go func() {
// 		// gracefully shutdown the server
// 		for range ch {
// 			_ = srv.Shutdown(ctx)
// 			<-ctx.Done()
// 		}
//
// 		_, cancel := context.WithTimeout(ctx, 5*time.Second) //nolint:gomnd
// 		defer cancel()
//
// 		_ = srv.Shutdown(ctx)
// 	}()
//
// 	err := srv.ListenAndServe()
// 	if err != nil {
// 		log.Errorf("serve metrics http server error: %v", err)
// 	}
// }
