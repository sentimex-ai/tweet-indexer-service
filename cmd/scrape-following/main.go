// Command scrape-following scrapes the full following list of public X
// handles into MongoDB (following.indexes), using the dummy.accounts pool.
//
//	scrape-following scrape [flags] handle [handle...]
//	scrape-following add-account --username NAME --cookies-file FILE
//	scrape-following accounts [list | restore --from-group N]
//	scrape-following check [--handle x]
//	scrape-following discover
//	scrape-following show handle [--limit 20]
//	scrape-following runs [--limit 20]
//
// Run from the repo root (config is read from ./config/env/ by default).
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"text/tabwriter"
	"time"

	"github.com/jeetdotso/tweet-indexer-service/config"
	"github.com/jeetdotso/tweet-indexer-service/dummyaccount"
	"github.com/jeetdotso/tweet-indexer-service/followscrape"
	"github.com/jeetdotso/tweet-indexer-service/persistence/mongodb"
	"github.com/jeetdotso/tweet-indexer-service/scrape/xclient"
	"github.com/rs/zerolog"
	"github.com/tebeka/selenium"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func usage() {
	fmt.Fprint(os.Stderr, `usage: scrape-following <command> [flags]

commands:
  scrape       scrape full following lists:   scrape [--resume] [--no-prune] [--file handles.txt] handle...
  add-account  store a browser session:       add-account --username NAME --cookies-file cookies.json
  accounts     list pool / restore retired:   accounts list | accounts restore --from-group 2
  check        test UserByScreenName + one Following page with a pool account
  discover     read current query ids from the logged-in x.com bundle
  show         print stored following of a handle:  show handle [--limit 20]
  runs         recent scrape runs

global flags (before the command): --config ./config/env/  --json
`)
}

func main() {
	gf := flag.NewFlagSet("global", flag.ExitOnError)
	cfgPath := gf.String("config", "./config/env/", "config directory")
	jsonLogs := gf.Bool("json", false, "JSON logs instead of console format")
	verbose := gf.Bool("v", false, "debug logging")
	gf.Usage = usage
	_ = gf.Parse(os.Args[1:])
	args := gf.Args()
	if len(args) == 0 {
		usage()
		os.Exit(2)
	}

	var log zerolog.Logger
	if *jsonLogs {
		log = zerolog.New(os.Stderr).With().Timestamp().Logger()
	} else {
		fi, _ := os.Stderr.Stat()
		tty := fi != nil && fi.Mode()&os.ModeCharDevice != 0
		log = zerolog.New(zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: "15:04:05", NoColor: !tty}).With().Timestamp().Logger()
	}
	if *verbose {
		log = log.Level(zerolog.DebugLevel)
	} else {
		log = log.Level(zerolog.InfoLevel)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	cfg := config.Load(*cfgPath)
	conn := mongodb.NewConnection(&cfg.Persistence)
	pctx, pcancel := context.WithTimeout(ctx, 10*time.Second)
	if err := conn.Session.Ping(pctx, nil); err != nil {
		pcancel()
		log.Fatal().Err(err).Str("database", cfg.Persistence.MongoDB.Database).Msg("cannot reach MongoDB (set MONGODB_URI)")
	}
	pcancel()
	deps, err := followscrape.NewDeps(ctx, cfg, &conn, log)
	if err != nil {
		log.Fatal().Err(err).Msg("init")
	}

	cmd, rest := args[0], args[1:]
	var cerr error
	switch cmd {
	case "scrape":
		cerr = cmdScrape(ctx, deps, rest)
	case "add-account":
		cerr = cmdAddAccount(ctx, deps, rest)
	case "accounts":
		cerr = cmdAccounts(ctx, deps, &conn, rest)
	case "check":
		cerr = cmdCheck(ctx, deps, rest)
	case "discover":
		cerr = cmdDiscover(ctx, deps, rest)
	case "show":
		cerr = cmdShow(ctx, &conn, rest)
	case "runs":
		cerr = cmdRuns(ctx, deps, rest)
	default:
		usage()
		os.Exit(2)
	}
	if cerr != nil {
		log.Error().Err(cerr).Msg(cmd + " failed")
		os.Exit(1)
	}
}

// ---------------------------------------------------------------------------

func cmdScrape(ctx context.Context, d *followscrape.Deps, args []string) error {
	fs := flag.NewFlagSet("scrape", flag.ExitOnError)
	file := fs.String("file", "", "file with one handle per line")
	resume := fs.Bool("resume", false, "continue the latest partial run of each handle from its saved cursor")
	noPrune := fs.Bool("no-prune", false, "keep edges a complete run did not see (by default they are removed: unfollows)")
	maxWait := fs.Duration("max-wait", 20*time.Minute, "longest wait for a rate-limited account to free up")
	pageSize := fs.Int("page-size", 0, "override x.scrape.pageSize")
	maxPages := fs.Int("max-pages", 0, "safety cap on pages per handle (0 = no cap)")
	_ = fs.Parse(args)

	handles := fs.Args()
	if *file != "" {
		f, err := os.Open(*file)
		if err != nil {
			return err
		}
		sc := bufio.NewScanner(f)
		for sc.Scan() {
			if h := strings.TrimSpace(sc.Text()); h != "" && !strings.HasPrefix(h, "#") {
				handles = append(handles, h)
			}
		}
		f.Close()
	}
	if len(handles) == 0 {
		return errors.New("no handles given")
	}

	opts := d.Options()
	opts.Resume = *resume
	opts.Prune = !*noPrune
	opts.MaxPoolWait = *maxWait
	opts.MaxPages = *maxPages
	if *pageSize > 0 {
		opts.PageSize = *pageSize
	}
	runs := d.NewScraper(opts).ScrapeAll(ctx, handles)

	fmt.Println()
	tw := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "HANDLE\tUSER_ID\tSTATUS\tEXPECTED\tSCRAPED\tSTORED\tNEW\tUPDATED\tPRUNED\tPAGES\tSECS\tERROR")
	failed := 0
	for _, r := range runs {
		if r.Status != followscrape.StatusComplete {
			failed++
		}
		fmt.Fprintf(tw, "%s\t%s\t%s\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%.0f\t%s\n", r.Handle, r.UserID, r.Status,
			r.Expected, r.Scraped, r.Stored, r.Inserted, r.Updated, r.Pruned, r.Pages, r.DurationSec, r.ErrorKind)
	}
	tw.Flush()
	if failed > 0 {
		return fmt.Errorf("%d of %d handles did not complete", failed, len(runs))
	}
	return nil
}

// cookies file formats accepted:
//   - JSON array from a browser cookie exporter: [{"name":"auth_token","value":"..."}, ...]
//   - JSON object: {"auth_token":"...","ct0":"..."}
//   - a Cookie header string: auth_token=...; ct0=...
//
// or env X_AUTH_TOKEN / X_CT0 when --cookies-file is omitted.
// Values are never printed.
func cmdAddAccount(ctx context.Context, d *followscrape.Deps, args []string) error {
	fs := flag.NewFlagSet("add-account", flag.ExitOnError)
	user := fs.String("username", "", "X username of the pool account")
	file := fs.String("cookies-file", "", "exported cookies (see docs); omit to read X_AUTH_TOKEN / X_CT0")
	verify := fs.Bool("verify", true, "call X once to confirm the session works")
	_ = fs.Parse(args)
	if *user == "" {
		return errors.New("--username is required")
	}
	var c map[string]string
	if *file != "" {
		raw, err := os.ReadFile(*file)
		if err != nil {
			return err
		}
		if c, err = parseCookies(raw); err != nil {
			return err
		}
	} else {
		c = map[string]string{"auth_token": os.Getenv("X_AUTH_TOKEN"), "ct0": os.Getenv("X_CT0"), "twid": os.Getenv("X_TWID")}
	}
	if c["auth_token"] == "" || c["ct0"] == "" {
		return errors.New("auth_token and ct0 cookies are required")
	}
	if err := d.Pool.UpsertCookieAccount(ctx, *user, c["auth_token"], c["ct0"], c["twid"]); err != nil {
		return err
	}
	d.Log.Info().Str("account", followscrape.Mask(*user)).Msg("account stored (cookie values not logged)")
	if !*verify {
		return nil
	}
	cl, err := d.ClientFor(&dummyaccount.SelAccount{UserName: *user, Cookies: cookiesToSelenium(c)})
	if err != nil {
		return err
	}
	u, _, err := cl.UserByScreenName(ctx, "x")
	if err != nil {
		return fmt.Errorf("stored, but verification failed (%s): %w", xclient.KindOf(err), err)
	}
	d.Log.Info().Str("probe", "@"+u.Handle).Str("user_id", u.ID).Msg("session works")
	return nil
}

func parseCookies(raw []byte) (map[string]string, error) {
	out := map[string]string{}
	s := strings.TrimSpace(string(raw))
	switch {
	case strings.HasPrefix(s, "["):
		var arr []struct{ Name, Value string }
		if err := json.Unmarshal([]byte(s), &arr); err != nil {
			return nil, fmt.Errorf("cookies json array: %w", err)
		}
		for _, c := range arr {
			out[c.Name] = c.Value
		}
	case strings.HasPrefix(s, "{"):
		if err := json.Unmarshal([]byte(s), &out); err != nil {
			return nil, fmt.Errorf("cookies json object: %w", err)
		}
	default:
		for _, part := range strings.Split(s, ";") {
			k, v, ok := strings.Cut(strings.TrimSpace(part), "=")
			if ok {
				out[strings.TrimSpace(k)] = strings.TrimSpace(v)
			}
		}
	}
	return out, nil
}

func cookiesToSelenium(c map[string]string) []selenium.Cookie {
	var out []selenium.Cookie
	for _, k := range []string{"auth_token", "ct0", "twid"} {
		if c[k] != "" {
			out = append(out, selenium.Cookie{Name: k, Value: c[k]})
		}
	}
	return out
}

func cmdAccounts(ctx context.Context, d *followscrape.Deps, conn *mongodb.MongoConnection, args []string) error {
	sub := "list"
	if len(args) > 0 {
		sub, args = args[0], args[1:]
	}
	switch sub {
	case "list":
		accts, err := d.Pool.List(ctx)
		if err != nil {
			return err
		}
		tw := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
		fmt.Fprintln(tw, "USERNAME\tGROUP\tIN_USE\tCOOKIES\tCOOLDOWN_UNTIL\tLAST_ERROR\tLAST_ERROR_AT")
		for _, a := range accts {
			cd := ""
			if a.RateLimitedUntil.After(time.Now()) {
				cd = a.RateLimitedUntil.Local().Format(time.DateTime)
			}
			at := ""
			if !a.LastErrorAt.IsZero() {
				at = a.LastErrorAt.Local().Format(time.DateTime)
			}
			fmt.Fprintf(tw, "%s\t%s\t%v\t%d\t%s\t%s\t%s\n", a.UserName, groupName(a.Group), a.InUse, a.CookieCount, cd, a.LastError, at)
		}
		return tw.Flush()
	case "restore":
		fs := flag.NewFlagSet("restore", flag.ExitOnError)
		from := fs.Int("from-group", -1, "group to move back to the working group (1=suspended 2=not_authenticated 3=not_working)")
		_ = fs.Parse(args)
		if *from < 0 {
			return errors.New("--from-group is required")
		}
		res, err := conn.Datastore.Collection(dummyaccount.Collection).UpdateMany(ctx,
			bson.M{"group": *from},
			bson.M{"$set": bson.M{"group": d.XCfg.Scrape.AccountGroup, "in_use": false, "rate_limited_until": time.Time{}}})
		if err != nil {
			return err
		}
		fmt.Printf("restored %d accounts from group %s\n", res.ModifiedCount, groupName(*from))
		return nil
	}
	return fmt.Errorf("unknown accounts subcommand %q", sub)
}

func groupName(g int) string {
	switch g {
	case dummyaccount.WORKING_GROUP:
		return "0 working"
	case dummyaccount.SUSPENDED_GROUP:
		return "1 suspended"
	case dummyaccount.NOT_AUTHENTICATED_GROUP:
		return "2 not_authenticated"
	case dummyaccount.NOT_WORKING_GROUP:
		return "3 not_working"
	}
	return fmt.Sprint(g)
}

// check leases one account and exercises both operations once. It is the
// fastest way to tell "query ids are stale" from "accounts are dead".
func cmdCheck(ctx context.Context, d *followscrape.Deps, args []string) error {
	fs := flag.NewFlagSet("check", flag.ExitOnError)
	handle := fs.String("handle", "x", "public handle to probe")
	_ = fs.Parse(args)
	acct, err := d.Pool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer d.Pool.Release(context.WithoutCancel(ctx), acct.UserName)
	cl, err := d.ClientFor(acct)
	if err != nil {
		return err
	}
	fmt.Printf("account %s\n", followscrape.Mask(acct.UserName))

	u, ri, err := cl.UserByScreenName(ctx, *handle)
	report("UserByScreenName", d.Ops.QueryID(xclient.OpUserByScreenName), ri, err)
	if err != nil {
		return err
	}
	fmt.Printf("  -> @%s id=%s following=%d followers=%d protected=%v\n", u.Handle, u.ID, u.FollowingCount, u.FollowerCount, u.Protected)

	p, ri, err := cl.FollowingPage(ctx, u.ID, u.Handle, "", d.XCfg.Scrape.PageSize)
	report("Following", d.Ops.QueryID(xclient.OpFollowing), ri, err)
	if err != nil {
		return err
	}
	fmt.Printf("  -> %d users on first page, next cursor present=%v\n", len(p.Users), p.NextCursor != "")
	for i, fu := range p.Users {
		if i == 3 {
			break
		}
		fmt.Printf("     @%s (%s) followers=%d\n", fu.Handle, fu.DisplayName, fu.FollowerCount)
	}
	return nil
}

func report(op, qid string, ri xclient.RateInfo, err error) {
	status := "OK"
	if err != nil {
		status = "FAIL " + xclient.KindOf(err).String() + ": " + err.Error()
	}
	fmt.Printf("%-17s queryId=%s rate=%d/%d  %s\n", op, qid, ri.Remaining, ri.Limit, status)
}

func cmdDiscover(ctx context.Context, d *followscrape.Deps, args []string) error {
	acct, err := d.Pool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer d.Pool.Release(context.WithoutCancel(ctx), acct.UserName)
	cl, err := d.ClientFor(acct)
	if err != nil {
		return err
	}
	found, err := cl.Discover(ctx, []string{xclient.OpUserByScreenName, xclient.OpFollowing}, 400)
	if err != nil {
		return err
	}
	fmt.Println("# current x.com values (paste into config/env/app.yaml under x.ops)")
	for _, name := range []string{xclient.OpUserByScreenName, xclient.OpFollowing} {
		dsc, ok := found[name]
		if !ok {
			fmt.Printf("# %s: not found in bundle\n", name)
			continue
		}
		changed := ""
		if dsc.QueryID != d.Ops.QueryID(name) {
			changed = "   # CHANGED, config has " + d.Ops.QueryID(name)
		}
		fmt.Printf("%s:\n  queryId: %s%s\n", name, dsc.QueryID, changed)
		if len(dsc.FeatureSwitches) > 0 {
			fmt.Printf("  # feature switches the bundle sends (set values from a real request):\n  #   %s\n", strings.Join(dsc.FeatureSwitches, ", "))
		}
		if len(dsc.FieldToggles) > 0 {
			fmt.Printf("  # field toggles: %s\n", strings.Join(dsc.FieldToggles, ", "))
		}
	}
	return nil
}

func cmdShow(ctx context.Context, conn *mongodb.MongoConnection, args []string) error {
	fs := flag.NewFlagSet("show", flag.ExitOnError)
	limit := fs.Int64("limit", 20, "rows to print")
	// allow `show handle --limit 5`
	var handle string
	if len(args) > 0 && !strings.HasPrefix(args[0], "-") {
		handle, args = args[0], args[1:]
	}
	_ = fs.Parse(args)
	if handle == "" && fs.NArg() > 0 {
		handle = fs.Arg(0)
	}
	handle = strings.ToLower(xclient.NormalizeHandle(handle))
	if handle == "" {
		return errors.New("usage: show <handle>")
	}
	coll := conn.Datastore.Collection("following.indexes")
	n, err := coll.CountDocuments(ctx, bson.M{"source_handle": handle})
	if err != nil {
		return err
	}
	dupes, err := coll.Aggregate(ctx, []bson.M{
		{"$match": bson.M{"source_handle": handle}},
		{"$group": bson.M{"_id": "$following_id", "n": bson.M{"$sum": 1}}},
		{"$match": bson.M{"n": bson.M{"$gt": 1}}},
		{"$count": "dupes"},
	})
	if err != nil {
		return err
	}
	var dc []struct{ Dupes int }
	_ = dupes.All(ctx, &dc)
	nd := 0
	if len(dc) > 0 {
		nd = dc[0].Dupes
	}
	fmt.Printf("@%s follows %d stored accounts (duplicate following_ids: %d)\n", handle, n, nd)
	cur, err := coll.Find(ctx, bson.M{"source_handle": handle},
		options.Find().SetSort(bson.M{"follower_count": -1}).SetLimit(*limit))
	if err != nil {
		return err
	}
	tw := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "_ID\tHANDLE\tDISPLAY_NAME\tFOLLOWERS\tFOLLOWING\tCAPTURED_AT")
	for cur.Next(ctx) {
		var r struct {
			ID             string    `bson:"_id"`
			Handle         string    `bson:"handle"`
			DisplayName    string    `bson:"display_name"`
			FollowerCount  int       `bson:"follower_count"`
			FollowingCount int       `bson:"following_count"`
			CapturedAt     time.Time `bson:"captured_at"`
		}
		if err := cur.Decode(&r); err != nil {
			return err
		}
		fmt.Fprintf(tw, "%s\t@%s\t%s\t%d\t%d\t%s\n", r.ID, r.Handle, r.DisplayName, r.FollowerCount, r.FollowingCount, r.CapturedAt.UTC().Format(time.RFC3339))
	}
	return tw.Flush()
}

func cmdRuns(ctx context.Context, d *followscrape.Deps, args []string) error {
	fs := flag.NewFlagSet("runs", flag.ExitOnError)
	limit := fs.Int64("limit", 20, "rows")
	_ = fs.Parse(args)
	runs, err := d.Runs.Recent(ctx, *limit)
	if err != nil {
		return err
	}
	tw := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "STARTED\tHANDLE\tSTATUS\tEXPECTED\tSCRAPED\tSTORED\tPAGES\tACCOUNTS\tERROR")
	for _, r := range runs {
		fmt.Fprintf(tw, "%s\t%s\t%s\t%d\t%d\t%d\t%d\t%d\t%s\n", r.StartedAt.Local().Format(time.DateTime), r.Handle, r.Status,
			r.Expected, r.Scraped, r.Stored, r.Pages, r.Accounts, r.Error)
	}
	return tw.Flush()
}
