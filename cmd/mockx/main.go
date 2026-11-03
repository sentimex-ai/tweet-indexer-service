// Command mockx serves the fake X API from followscrape/mockx so the scraper can
// be demonstrated end to end without a real X account.
//
//	go run ./cmd/mockx -addr 127.0.0.1:8099
package main

import (
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"

	"github.com/jeetdotso/tweet-indexer-service/followscrape/mockx"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:8099", "listen address")
	limit := flag.Int("limit", 15, "Following calls per window before HTTP 429 (0 = off)")
	window := flag.Int("window", 20, "rate limit window in seconds")
	flag.Parse()

	cfg := mockx.Config{
		Profiles: []mockx.Profile{
			{Handle: "small_account", ID: "1001", Name: "Small Account", Following: 137},
			{Handle: "mid_account", ID: "1002", Name: "Mid Account", Following: 640},
			{Handle: "big_follower", ID: "1003", Name: "Big Follower", Following: 2350},
			{Handle: "private_person", ID: "1004", Name: "Private", Following: 50, Protected: true},
			{Handle: "banned_person", ID: "1005", Name: "Banned", Following: 5, Suspended: true},
		},
		ValidTokens:    []string{"tok-a", "tok-b", "tok-c"},
		LimitPerWindow: *limit,
		WindowSeconds:  *window,
		DieAfter:       map[string]int{"tok-c": 4}, // third account's session expires after 4 calls
	}
	s := mockx.New(cfg)
	s.Close() // we want our own listener on a fixed address
	ln, err := net.Listen("tcp", *addr)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	srv := &http.Server{Handler: s.Config.Handler}
	go srv.Serve(ln)
	fmt.Printf("mockx listening on http://%s (accounts: tok-a tok-b tok-c)\n", *addr)
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	<-c
	srv.Close()
}
