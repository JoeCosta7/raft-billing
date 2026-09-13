package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"raft-biling/internal/config"
	"raft-biling/internal/node"
	"syscall"
	"time"
	_ "time/tzdata"
)

//All config flags
//based on a cli start command of
/*scheduler start \
  --node-id=node1 \
  --raft-addr=127.0.0.1:7001 \
  --http-addr=127.0.0.1:8001 \
  --data-dir=/var/lib/scheduler/node1 \
  --peers=node1=127.0.0.1:7001,node2=127.0.0.1:7002,node3=127.0.0.1:7003 \
  --bootstrap
*/

func main() {

	//creates config with cli args
	cfg, err := config.New(os.Args[1:])
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Printf("config: %+v\n", cfg)
	//builds node from config
	n, err := node.New(cfg)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Printf("node constructed: %+v\n", n)
	// runtime context for detecting e.g. Ctrl + C
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	if err := n.Start(ctx); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Printf("scheduler ready on %s\n", cfg.HTTPAddr)

	<-ctx.Done()

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer shutdownCancel()
	log.Println("shutting down")
	if err := n.Shutdown(shutdownCtx); err != nil {
		log.Println(err)
	}
}
