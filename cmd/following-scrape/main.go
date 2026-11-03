// Command following-scrape reads the full list of accounts an X profile follows
// and stores it in MongoDB (collection following.indexes, _id = userID_followingID).
//
//	following-scrape add-account --username acct1 --cookies-file ./acct1.cookies.json
//	following-scrape accounts
//	following-scrape scrape --handle jack,@elonmusk --file more-handles.txt
//	following-scrape count --handle jack
//
// It needs MongoDB and at least one logged in X account in dummy.accounts. It
// does not start any server, the tweet pipeline, Grok or Kafka.
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net/url"
	"os"
	"os/signal"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/jeetdotso/tweet-indexer-service/config"
	"github.com/jeetdotso/tweet-indexer-service/dummyaccount"
	"github.com/jeetdotso/tweet-indexer-service/followscrape"
	"github.com/jeetdotso/tweet-indexer-service/indexer/repository"
	"github.com/jeetdotso/tweet-indexer-service/persistence/mongodb"
	twitterAPI "github.com/jeetdotso/tweet-indexer-service/scrape/internals/api"
	"github.com/tebeka/selenium"
	"go.mongodb.org/mongo-driver/bson"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	cmd, args := os.Args[1], os.Args[2:]
	fs := flag.NewFlagSet(cmd, flag.ExitOnError)
	cfgDir := fs.String("config-dir", "./config/env/", "directory with app.yaml and app-<ENVIRONMENT>.yaml")
	allowRemote := fs.Bool("allow-remote", false, "allow a MongoDB that is not on this machine")

	var (
		handles, file, cursor, username, cookiesFile *string
		prune, asJSON                                *bool
		workers                                      *int
	)
	switch cmd {
	case "scrape":
		handles = fs.String("handle", "", "comma separated handles (@jack, jack, x.com/jack)")
		file = fs.String("file", "", "file with one handle per line")
		cursor = fs.String("resume-cursor", "", "continue a stopped run from this cursor (one handle only)")
		prune = fs.Bool("prune", false, "after a complete run delete stored edges that X no longer lists")
		asJSON = fs.Bool("json", false, "print one JSON result per handle on stdout")
		workers = fs.Int("workers", 1, "handles scraped at the same time (each uses its own account)")
	case "count":
		handles = fs.String("handle", "", "handle to count")
	case "add-account":
		username = fs.String("username", "", "account name (just a label, used as the key)")
		cookiesFile = fs.String("cookies-file", "", "JSON with the session cookies: a browser export array, or {\"auth_token\":\"..\",\"ct0\":\"..\"}")
	case "accounts":
	default:
		usage()
		os.Exit(2)
	}
	_ = fs.Parse(args)

	if os.Getenv("ENVIRONMENT") == "" {
		_ = os.Setenv("ENVIRONMENT", "local") // never fall into the production-looking development profile
	}
	cfg := config.Load(*cfgDir)
	if !*allowRemote && !isLocal(cfg.Persistence.MongoDB.URL) {
		fatal("refusing to use a MongoDB that is not on this machine (%s). Set MONGODB_URI to a local one, or pass --allow-remote if you really mean it.", redact(cfg.Persistence.MongoDB.URL))
	}
	if v := os.Getenv("X_BASE_URL"); v != "" { // point the tool at a fake X (cmd/mockx) for demos and tests
		cfg.X.BaseURL = v
	}
	twitterAPI.Configure(cfg.X)
	conn := mongodb.NewConnection(&cfg.Persistence)
	svc := dummyaccount.NewSeleniumService(&conn, cfg)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	switch cmd {
	case "add-account":
		addAccount(ctx, svc, *username, *cookiesFile)
	case "accounts":
		listAccounts(ctx, &conn)
	case "count":
		repo := repository.NewFollowingRepository(&conn)
		for _, h := range splitHandles(*handles, "") {
			n, _ := repo.CountByUserID(ctx, mustID(ctx, svc, cfg, h))
			fmt.Printf("%s: %d stored\n", h, n)
		}
	case "scrape":
		list := splitHandles(*handles, *file)
		if len(list) == 0 {
			fatal("give --handle or --file")
		}
		if *cursor != "" && len(list) != 1 {
			fatal("--resume-cursor works with exactly one handle")
		}
		os.Exit(runScrape(ctx, cfg, svc, &conn, list, *cursor, *prune, *asJSON, *workers))
	}
}

func runScrape(ctx context.Context, cfg *config.Config, svc dummyaccount.Service, conn *mongodb.MongoConnection,
	list []string, cursor string, prune, asJSON bool, workers int) int {
	fc := cfg.FollowScrape
	opt := followscrape.Options{
		PageSize:           fc.PageSize,
		MaxPages:           fc.MaxPages,
		MaxAttemptsPerPage: fc.MaxAttemptsPerPage,
		PageDelay:          time.Duration(fc.PageDelayMs) * time.Millisecond,
		RateLimitMaxWait:   time.Duration(fc.RateLimitMaxWaitS) * time.Second,
		Cooldown:           time.Duration(fc.CooldownMinutes) * time.Minute,
		Prune:              prune,
		Logf: func(f string, a ...interface{}) {
			fmt.Fprintf(os.Stderr, "%s  %s\n", time.Now().Format("15:04:05"), fmt.Sprintf(f, a...))
		},
	}
	pool := &followscrape.MongoPool{Svc: svc, BaseURL: cfg.X.BaseURL, ProxyURL: cfg.X.ProxyURL}
	sc := followscrape.New(pool, repository.NewFollowingRepository(conn), opt)

	if workers < 1 {
		workers = 1
	}
	if workers > 3 {
		workers = 3 // each worker holds an account; X punishes bursts
	}
	type job struct {
		i int
		h string
	}
	jobs := make(chan job)
	results := make([]*followscrape.Result, len(list))
	errs := make([]error, len(list))
	done := make(chan struct{})
	for w := 0; w < workers; w++ {
		go func() {
			for j := range jobs {
				results[j.i], errs[j.i] = sc.Scrape(ctx, j.h, cursor)
			}
			done <- struct{}{}
		}()
	}
	for i, h := range list {
		jobs <- job{i, h}
	}
	close(jobs)
	for w := 0; w < workers; w++ {
		<-done
	}

	failed := 0
	for i, r := range results {
		if asJSON {
			b, _ := json.Marshal(r)
			fmt.Println(string(b))
		}
		status := "OK      "
		if errs[i] != nil {
			status, failed = "FAILED  ", failed+1
		} else if !r.Complete {
			status, failed = "PARTIAL ", failed+1
		}
		fmt.Fprintf(os.Stderr, "%s%-24s scraped=%d expected~%d stored=%d pages=%d rate_limited=%d swapped=%d",
			status, "@"+r.Handle, r.Scraped, r.ExpectedCount, r.StoredTotal, r.Pages, r.RateLimited, r.AccountsSwaped)
		if errs[i] != nil {
			fmt.Fprintf(os.Stderr, "\n          error: %v", errs[i])
		}
		if r.ResumeCursor != "" {
			fmt.Fprintf(os.Stderr, "\n          resume with: --handle %s --resume-cursor %q", r.Handle, r.ResumeCursor)
		}
		fmt.Fprintln(os.Stderr)
	}
	if failed > 0 {
		return 1
	}
	return 0
}

func addAccount(ctx context.Context, svc dummyaccount.Service, user, file string) {
	if user == "" || file == "" {
		fatal("add-account needs --username and --cookies-file")
	}
	raw, err := os.ReadFile(file)
	if err != nil {
		fatal("read cookies file: %v", err)
	}
	cookies, err := parseCookies(raw)
	if err != nil {
		fatal("%v", err)
	}
	if err := svc.NewAccount(ctx, user, "", dummyaccount.WORKING_GROUP, "manual"); err != nil {
		fatal("create account: %v", err)
	}
	_ = svc.RemoveCookies(ctx, user) // a re-add replaces the cookies instead of stacking them
	if err := svc.JustAddCookies(ctx, user, cookies); err != nil {
		fatal("save cookies: %v", err)
	}
	if err := svc.LogOut(ctx, user); err != nil { // JustAddCookies leaves the row marked in use
		fatal("release account: %v", err)
	}
	fmt.Printf("added %s with %d cookies (values not shown)\n", user, len(cookies))
}

// parseCookies reads a browser export ([{name,value,domain,path,secure,expirationDate}]) or a
// plain {name: value} object. auth_token and ct0 are required: without both the session cannot work.
func parseCookies(raw []byte) ([]selenium.Cookie, error) {
	var out []selenium.Cookie
	var arr []map[string]interface{}
	if err := json.Unmarshal(raw, &arr); err == nil {
		for _, m := range arr {
			name, _ := m["name"].(string)
			val, _ := m["value"].(string)
			if name == "" {
				continue
			}
			c := selenium.Cookie{Name: name, Value: val, Domain: ".x.com", Path: "/", Secure: true}
			if d, ok := m["domain"].(string); ok && d != "" {
				c.Domain = d
			}
			if p, ok := m["path"].(string); ok && p != "" {
				c.Path = p
			}
			for _, k := range []string{"expirationDate", "expiry", "expires"} {
				if f, ok := m[k].(float64); ok && f > 0 {
					c.Expiry = uint(f)
				}
			}
			out = append(out, c)
		}
	} else {
		var obj map[string]string
		if err := json.Unmarshal(raw, &obj); err != nil {
			return nil, errors.New("cookies file is neither a JSON array nor a {name:value} object")
		}
		for k, v := range obj {
			out = append(out, selenium.Cookie{Name: k, Value: v, Domain: ".x.com", Path: "/", Secure: true})
		}
	}
	have := map[string]bool{}
	for _, c := range out {
		if c.Value != "" {
			have[c.Name] = true
		}
	}
	if !have["auth_token"] || !have["ct0"] {
		return nil, errors.New("cookies must include non-empty auth_token and ct0")
	}
	return out, nil
}

func listAccounts(ctx context.Context, conn *mongodb.MongoConnection) {
	cur, err := conn.Datastore.Collection(dummyaccount.Collection).Find(ctx, bson.M{})
	if err != nil {
		fatal("%v", err)
	}
	var rows []dummyaccount.SelAccount
	if err := cur.All(ctx, &rows); err != nil {
		fatal("%v", err)
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].UserName < rows[j].UserName })
	names := map[int]string{dummyaccount.WORKING_GROUP: "working", dummyaccount.SUSPENDED_GROUP: "suspended",
		dummyaccount.NOT_AUTHENTICATED_GROUP: "not-authenticated", dummyaccount.NOT_WORKING_GROUP: "not-working"}
	fmt.Printf("%-24s %-18s %-7s %-8s %s\n", "ACCOUNT", "GROUP", "IN USE", "COOKIES", "RESTING UNTIL")
	for _, r := range rows {
		g := names[r.Group]
		if g == "" {
			g = fmt.Sprintf("group %d", r.Group)
		}
		rest := "-"
		if r.CooldownUntil.After(time.Now()) {
			rest = r.CooldownUntil.Format(time.RFC3339)
		}
		fmt.Printf("%-24s %-18s %-7v %-8d %s\n", r.UserName, g, r.InUse, len(r.Cookies), rest)
	}
}

func mustID(ctx context.Context, svc dummyaccount.Service, cfg *config.Config, handle string) string {
	pool := &followscrape.MongoPool{Svc: svc, BaseURL: cfg.X.BaseURL, ProxyURL: cfg.X.ProxyURL}
	a, err := pool.Acquire(ctx)
	if err != nil {
		fatal("need one account to look up %s: %v", handle, err)
	}
	defer pool.Release(ctx, a)
	r, err := a.Client().FullUser(handle)
	if err != nil {
		fatal("look up %s: %v", handle, err)
	}
	return r.Get("data.user.result.rest_id").String()
}

func splitHandles(csv, file string) []string {
	var out []string
	add := func(s string) {
		if h := followscrape.NormalizeHandle(s); h != "" {
			out = append(out, h)
		}
	}
	for _, s := range strings.Split(csv, ",") {
		add(s)
	}
	if file != "" {
		f, err := os.Open(file)
		if err != nil {
			fatal("read %s: %v", file, err)
		}
		defer f.Close()
		sc := bufio.NewScanner(f)
		for sc.Scan() {
			if line := strings.TrimSpace(sc.Text()); line != "" && !strings.HasPrefix(line, "#") {
				add(line)
			}
		}
	}
	return out
}

func isLocal(mongoURL string) bool {
	u, err := url.Parse(mongoURL)
	if err != nil {
		return false
	}
	h := u.Hostname()
	return h == "localhost" || h == "127.0.0.1" || h == "::1"
}

func redact(mongoURL string) string {
	if u, err := url.Parse(mongoURL); err == nil {
		return u.Scheme + "://" + u.Hostname()
	}
	return "(unparseable)"
}

func fatal(f string, a ...interface{}) {
	fmt.Fprintf(os.Stderr, "error: "+f+"\n", a...)
	os.Exit(1)
}

func usage() {
	fmt.Fprintln(os.Stderr, `usage: following-scrape <command> [flags]
  add-account  --username NAME --cookies-file FILE   store a logged in X session
  accounts                                           list pooled accounts (no secrets)
  scrape       --handle a,b [--file F] [--prune] [--json] [--workers N] [--resume-cursor C]
  count        --handle a                            rows stored for a profile
common flags: --config-dir DIR  --allow-remote
needs MONGODB_URI (default mongodb://127.0.0.1:27017) and ENVIRONMENT=local (default)`)
}
