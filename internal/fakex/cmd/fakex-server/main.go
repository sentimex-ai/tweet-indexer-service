// fakex-server runs the fake X GraphQL API on a fixed port so the real
// scrape-following binary can be exercised end to end without touching
// x.com (CI, local dev, reviewing failure handling).
//
//	go run ./internal/fakex/cmd/fakex-server -addr 127.0.0.1:8787
//	X_BASE_URL=http://127.0.0.1:8787 go run ./cmd/scrape-following scrape fake_big
//
// Profiles: fake_small (37), fake_mid (480), fake_big (1500), fake_protected,
// fake_suspended, x (probe used by add-account). Sessions whose auth_token
// starts with "expired"/"suspended"/"ratelimited" behave accordingly
// ("ratelimited" accounts get 429 after 8 calls).
package main

import (
	"flag"
	"log"
	"net/http"
	"strings"

	"github.com/jeetdotso/tweet-indexer-service/internal/fakex"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:8787", "listen address")
	uq := flag.String("user-query-id", "32pL5BWe9WKeSK1MoPvFQQ", "UserByScreenName query id to accept")
	fq := flag.String("following-query-id", "gsxNGYhRKA6iYYSInE9qew", "Following query id to accept")
	flag.Parse()

	fx := fakex.New()
	fx.Close() // we serve fx.Handler on our own listener below
	fx.UserQueryID, fx.FollowingQueryID = *uq, *fq
	for _, p := range []fakex.Profile{
		{ID: "44196397", Handle: "x", Following: 3},
		{ID: "1001", Handle: "fake_small", Following: 37},
		{ID: "1002", Handle: "fake_mid", Following: 480},
		{ID: "1003", Handle: "fake_big", Following: 1500},
		{ID: "1004", Handle: "fake_protected", Following: 10, Protected: true},
		{ID: "1005", Handle: "fake_suspended", Suspended: true},
	} {
		fx.Add(p)
	}
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if c, err := r.Cookie("auth_token"); err == nil {
			fx.Locked(func() {
				switch {
				case strings.HasPrefix(c.Value, "expired"):
					fx.Expired[c.Value] = true
				case strings.HasPrefix(c.Value, "suspended"):
					fx.Suspended[c.Value] = true
				case strings.HasPrefix(c.Value, "ratelimited"):
					if _, ok := fx.RateLimitAfter[c.Value]; !ok {
						fx.RateLimitAfter[c.Value] = 8
					}
				}
			})
		}
		fx.Config.Handler.ServeHTTP(w, r)
		log.Printf("%s %s", r.Method, r.URL.Path)
	})
	log.Printf("fake X listening on http://%s", *addr)
	log.Fatal(http.ListenAndServe(*addr, h))
}
