# STATUS: following-list scraper

Give it a public X handle and it stores that profile's complete following list
in MongoDB (`following.indexes`). Tweet scraping, Grok and Kafka are switched
off; the service starts with nothing but MongoDB.

## Run a scrape locally

Prerequisites: Go 1.23, a MongoDB you can write to (local `mongod`, or
`docker run -d -p 27017:27017 mongo:7`), and **one logged-in X account**
used only for scraping (never a personal account).

```bash
# 0. from the repo root
export MONGODB_URI=mongodb://localhost:27017      # default
export MONGODB_DATABASE=tweet-indexer-dev         # default in development profile

# 1. export the test account's session from a browser where it is logged in:
#    DevTools -> Application -> Cookies -> https://x.com, copy auth_token and ct0
#    into a file (this file is .gitignored):
cat > cookies.json <<'EOF'
{"auth_token": "<value>", "ct0": "<value>"}
EOF
#    (a JSON array exported by a cookie-export extension, or a raw
#    "auth_token=...; ct0=..." string, also work; so do env vars X_AUTH_TOKEN / X_CT0)

# 2. store it in the pool. It also makes one test call, so you know it works
go run ./cmd/scrape-following add-account --username <account handle> --cookies-file cookies.json
rm cookies.json

# 3. check that the query ids still match the live x.com web app
go run ./cmd/scrape-following check --handle nasa
#   both lines must say OK. If one says stale_query: see "When X changes things" below.

# 4. scrape (one or many handles, or --file handles.txt)
go run ./cmd/scrape-following scrape nasa naval

# 5. inspect what was stored
go run ./cmd/scrape-following show nasa --limit 10
go run ./cmd/scrape-following runs
```

`scrape` prints a summary table and exits non-zero if any handle did not
complete. Re-running the same handle updates the existing documents in place.

### As an HTTP endpoint

The service (`go run .`) also exposes the scraper, protected by
`ADMIN_TOKEN`:

```bash
ADMIN_TOKEN=change-me go run .
curl -XPOST localhost:9090/admin/following/scrape -H 'X-Admin-Token: change-me' \
     -d '{"handles":["nasa","naval"]}'                      # 202 + batch id
curl localhost:9090/admin/following/batches/<id> -H 'X-Admin-Token: change-me'
curl 'localhost:9090/admin/following/runs?limit=5' -H 'X-Admin-Token: change-me'
```

Batches run one at a time, so parallel requests cannot hammer the account pool.

### Without an X account (fake X)

`internal/fakex` imitates X's two GraphQL endpoints, including its error
responses. It is used by the tests, and you can run it by hand:

```bash
go run ./internal/fakex/cmd/fakex-server &        # http://127.0.0.1:8787
export X_BASE_URL=http://127.0.0.1:8787
X_AUTH_TOKEN=t1 X_CT0=c1 go run ./cmd/scrape-following add-account --username fake1
go run ./cmd/scrape-following scrape fake_big fake_mid fake_protected
```

## What is stored

`following.indexes`, one document per (profile, followed account):

| field | |
|---|---|
| `_id` | `<user_id>_<following_id>`. Re-runs upsert, so there are no duplicates |
| `user_id`, `source_handle` | the profile whose list this is |
| `following_id`, `handle`, `display_name` | the followed account |
| `follower_count`, `following_count`, `verified`, `protected`, `profile_img` | snapshot of the followed account |
| `captured_at` | last scrape that saw this edge |
| `first_seen_at` | first scrape that saw it (set on insert only) |
| `run_id` | the run that last saw it |

When a run **completes**, edges of that profile that the run did not see are
deleted (they are unfollows). Use `--no-prune` to keep them. Partial runs never delete
anything.

`following.runs` has one document per scrape attempt: status
(`complete`/`partial`/`failed`/`protected`/`not_found`/`suspended`), counts,
error, timings and a cursor checkpoint. `scrape --resume` continues a partial
run from that cursor instead of starting over (useful for very long lists
after a rate limit).

`expected` (the profile's `friends_count`) is usually a little higher than
`stored`: X counts suspended/deactivated accounts in `friends_count` but returns them as
`UserUnavailable` in the list. Those are counted in `skipped`.

## What was fixed

1. **Parser dropped every account.** X moved `screen_name`/`name` from `legacy` to
   `core` in 2025. The old parser only read `legacy.*`, so every entry had an
   empty handle and was discarded. `xclient.ParseUser` reads both layouts.
2. **Pagination relied on entry positions** ("the last two entries are
   cursors"). Cursors are now found by `entryId` (`cursor-bottom-*`),
   `TimelineReplaceEntry` cursors are handled, and the loop stops on an end cursor
   (`0|…`), a repeated cursor, or two empty pages.
3. **Query ids and feature flags were hardcoded.** They now live in
   `config/env/app.yaml` under `x.ops`, as raw JSON pasted from DevTools (viper
   lower-cases YAML map keys, which would break camelCase field toggles). If X
   starts requiring a new feature flag, the client adds it for the running
   process and logs a WARN naming it.
4. **Every error was "bad request".** `xclient.Error` classifies responses by HTTP
   status and X error code into rate_limited / session_expired /
   account_suspended / account_locked / forbidden / stale_query /
   missing_features / target_not_found / target_protected / target_suspended /
   transient. See the policy table below.
5. **The service could not start locally.** `kafka.NewProducer` exits the process when
   `/etc/kafka/*.crt` is missing, and the admin controller built a Kafka
   producer at boot. Kafka is now behind `stream.kafka.enabled` (default
   off, no-op producer).
6. **Cookies leaked to stdout.** `DumpTransport` printed every request header,
   including `Cookie` and `x-csrf-token`, and `GetTweet` printed whole responses.
   Both are removed. The new client never logs or returns cookie values (tests
   assert this), and pool account names are masked in logs (`sc…42`).
7. **Following docs stored IDs only.** They now carry handle, display name,
   counts and timestamps, written with one unordered bulk upsert per page.
8. **`ResetAll()` at boot** released every account lease, including leases held by
   other running instances. The new pool uses lease timeouts instead
   (`x.scrape.leaseTimeoutS`); `ResetAll` only runs if the legacy tweet
   pipeline is enabled.
9. **The UA did not match the TLS fingerprint** (a Firefox UA on a Chrome-133 TLS
   profile). It now uses a Chrome 133 UA and matching `sec-ch-ua` headers.

## Failure policy (rate limits, dead sessions)

| X says | What happens |
|---|---|
| 429 / code 88 | account parked until `x-rate-limit-reset` (fallback 15 min); continue on the next account |
| `x-rate-limit-remaining: 0` on a success | account parked *before* it hits a 429 |
| all accounts cooling | wait for the earliest reset (`--max-wait`, default 20 min), otherwise stop as `partial` |
| 401 / code 32, 89, 215, 353 | account moved to group 2 `not_authenticated` |
| code 64 | account moved to group 1 `suspended` |
| code 326, redirect to `/account/access` | account moved to group 3 `not_working` |
| 403 without a code | 10 min cool-down only. Could be systemic, so the account is never retired for it |
| 5xx / network | exponential backoff with jitter, same account, up to 6 tries |
| 404 on the GraphQL path | `stale_query`: stop. Rotating accounts cannot fix a stale query ID |
| target protected / suspended / unknown | stop that handle; no account is touched |

At most 5 account swaps per handle. A **circuit breaker** stops the whole batch
when 3 different accounts in a row fail the same way with no success in
between. That pattern means X changed something (bearer token, headers, an IP
block), and continuing would retire the whole pool. Retired accounts can be
put back with `scrape-following accounts restore --from-group 2`.
`scrape-following accounts` lists the pool (no cookie values).

## What was removed or disabled

All of the following is behind `pipelines.*` in `config/env/app.yaml`, default off. No code was
deleted, so nothing the other services import breaks.

- Token tweet indexer and scrape-cycle scheduler (`pipelines.tweets`)
- Grok: grok-py calls, grok retry and profile-analysis schedulers (`pipelines.grok`).
  grok-py was also only needed for `x-client-transaction-id` on
  `UsersByRestIds`, which the following scraper does not call.
- Legacy cron jobs: account health, following refresh, profile updater (`pipelines.schedulers`)
- gRPC server and outbound clients (`pipelines.grpc`)
- Kafka producers (`stream.kafka.enabled`)

The legacy following refresh (`following_refresh.scheduler.go`) also called
`DeleteAllBeforeTime`, which deletes *every* profile's edges older than the
run start. Keep `pipelines.schedulers` off, or replace it with the new scraper,
before enabling it.

## Still fragile

- **Query IDs rotate.** The IDs in `app.yaml` were verified against live x.com on
  2026-11-04 (see "Live verification" below). `scrape-following check` tells you in one call whether they still work.
  `scrape-following discover` reads the current IDs from the logged-in x.com
  bundle, but bundle layout is X's choice, so treat it as a helper.
  The reliable fallback is DevTools: open `x.com/<handle>/following`, filter
  the Network tab for `graphql`, and copy the ID from `/i/api/graphql/<ID>/Following`
  plus the decoded `features` parameter.
- **`x-client-transaction-id`.** X requires this header on some GraphQL endpoints.
  `Following` and `UserByScreenName` did not need it as of the last working
  version. If X starts enforcing it there, requests will fail with
  `forbidden` (403) or `stale_query` (404) on every account, and the circuit breaker will
  stop the batch. A pure-Go generator would then be needed. grok-py's
  endpoint is the existing reference implementation, but it sends `auth_token`
  and `ct0` in a URL query string.
- **Very large lists.** X caps the users per Following page and the requests per
  account per 15-minute window. `check` prints the live window as
  `rate=remaining/limit`, and the scraper logs `rate_remaining` on every page.
  Size the pool from those numbers. A list of several thousand accounts needs
  dozens to hundreds of pages, so use 2+ pool accounts or `--resume`.
  X has also been known to stop serving very long lists before the end. A run
  then ends `complete` with `stored` noticeably below `expected`; check the
  `runs` table.
- **Account trust.** New or low-trust accounts get locked or challenged quickly. The pool
  marks them, but someone still has to refresh their cookies.
- **Secrets in git history.** The previous configs committed a production MongoDB
  URI with password, a DigitalOcean `AVNS_…` password and an X API bearer token.
  They are removed from the tree, but **they remain in git history and should be
  rotated.**
- The legacy `/public/indexer/admin/*` HTTP routes have no authentication
  (unchanged; they are outside this scope).

## Live verification (2026-11-04, one pool account)

```
UserByScreenName  queryId=32pL5BWe9WKeSK1MoPvFQQ rate=148/150  OK
Following         queryId=gsxNGYhRKA6iYYSInE9qew rate=499/500  OK

HANDLE   USER_ID     STATUS    EXPECTED  SCRAPED  STORED  NEW   UPDATED  PAGES  SECS
nasa     11348282    complete  115       113      113     113   0        3      4
sama     1605        complete  1034      1034     1034    1034  0        21     37
balajis  2178012643  complete  3937      3923     3923    3923  0        79     145

re-run:
sama     1605        complete  1034      1034     1034    0     1034     21     37
@sama follows 1034 stored accounts (duplicate following_ids: 0)
```

The gap between `expected` and `stored` (nasa 2, balajis 14) is accounts X
reports as unavailable (suspended or deactivated); they are counted in `skipped`.

## Tests

```bash
go test ./scrape/xclient/ ./followscrape/
```

The tests run the real client against the fake X server: a 1,234-account list over 25 pages, re-runs with
no duplicates, unfollow pruning, a 429 in the middle of a list (rotate and continue), expired
and suspended accounts, pool exhaustion then `--resume`, the circuit breaker, a
protected target, 503 backoff, automatic feature-flag repair, and that no cookie
value appears in errors.
