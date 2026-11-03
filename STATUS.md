# STATUS: following scraper

Goal: give the service a public X handle, get back that profile's full following list in MongoDB.

## Read this first: what was and was not tested

| | Status |
|---|---|
| Paging to the end, 1,000+ lists, rate limits, dead sessions, bad targets, re-runs | **Tested** against a fake X server (`cmd/mockx`) and a real local MongoDB. Logs in `docs/proof-mock-run.txt`. |
| Unit and integration tests | **Pass**: `go test ./followscrape/ ./scrape/... ./indexer/repository/` |
| The real x.com API (current query ids, feature flags, response layout, page size 100) | **Not tested.** I had no logged-in X account, so nothing was run against x.com. |

So the logic is proven, but the first real run on x.com may still need its query ids or feature flags updated (see "Still fragile"). The tool is built so that case fails fast and clearly (`x rejected the request format`) without burning the account pool.

## What I fixed

- **Query ids and feature flags are now config**, not code. See `x.operations` in `config/env/app.yaml`. Any id can be overridden without a rebuild: `X_QUERY_ID_FOLLOWING=...`, `X_QUERY_ID_USERBYSCREENNAME=...`. If X answers "The following features cannot be null: a, b", the client adds those flags as false and retries once.
- **Errors are no longer all "bad request".** Every failed X call becomes a typed error (`scrape/internals/api/errors.go`): rate limited (HTTP 429 or code 88, with the reset time from the headers), dead session (code 32/215/239/401), suspended account (64), locked account (326), target missing, target protected or suspended, stale request, temporary failure (5xx).
- **Paging is more reliable.** The old code assumed the next-page cursor was always the second to last row. Rows are now recognised by what they are (`user-` rows, bottom cursor), and the parser accepts both the old layout and the newer `user_result_by_screen_name` layout. Page size is 100 (was 20), so a 1,000 account list is about 10 requests.
- **Accounts are handled properly.** Rate limited: wait if the window is short, otherwise rest the account (`cooldown_until` on the account row, skipped by the pool) and switch. Dead, suspended or locked: moved to the existing `NOT_AUTHENTICATED` / `SUSPENDED` / `NOT_WORKING` groups. A bad target (missing, protected, suspended) is reported for that handle only and costs no account. A stale query id stops the whole run.
- **No cookies in logs.** `DumpTransport` used to print every request header (session cookie and csrf token) to stdout on `GetTweet`; it now prints nothing. Errors never carry the request. Tested with real runs: no cookie values anywhere in the output.
- **Storage.** `following.indexes`, `_id = userID_followingID`, written with upserts. Each row now also has `following_handle`, `following_name`, `following_followers_count`, `following_following_count`, `captured_at` (latest run) and `first_captured_at`. The old refresh job's `UpdateOrInsert` was rewritten so it cannot blank these fields.

## What I removed or switched off

Nothing was deleted. Everything below is now **off by default** via `features:` in `config/env/app.yaml` and the service starts with only MongoDB:

- tweet pipeline and its cron jobs (`features.tweetPipeline`)
- Grok (`features.grok`), no `grok-py` needed
- Kafka (`features.kafka`): producers become no-ops, so nothing dials Kafka or reads certificates
- gRPC clients and server (`features.grpcClients`, `features.grpcServer`)
- the account health cron (`features.accountHealth`)

Checked: `ENVIRONMENT=local ./service` starts, `GET /` returns `Tweet Indexer Health OK`, the admin list endpoint answers 200, and there is no Kafka or Grok output in the log.

## How to run a scrape locally

Needs Go 1.23 and a MongoDB on this machine.

```
# 1. MongoDB (any local one works)
docker run -d --name mongo -p 27017:27017 mongo:8
export MONGODB_URI=mongodb://127.0.0.1:27017

# 2. build, from the repo root
go build -o following-scrape ./cmd/following-scrape

# 3. add a logged in X account. Export its cookies from a browser to a JSON file
#    OUTSIDE the repo. It must contain auth_token and ct0, either as
#    {"auth_token":"...","ct0":"..."} or a browser export array.
./following-scrape add-account --username acct1 --cookies-file ~/acct1.cookies.json
./following-scrape accounts            # lists accounts, never shows cookies

# 4. scrape (handles, @handles or profile URLs; --file for a list)
./following-scrape scrape --handle jack,@someone
./following-scrape scrape --file handles.txt --json     # one JSON result per handle on stdout

# 5. check
./following-scrape count --handle jack
```

- Re-running a handle updates the rows (same `_id`), it never adds a second one. `--prune` also deletes stored follows that X no longer lists, after a complete run.
- A run that stops early prints `resume with: --handle X --resume-cursor "..."`.
- The tool uses the `local` config profile unless `ENVIRONMENT` is set, and **refuses to run against a MongoDB that is not on this machine** unless you pass `--allow-remote`.
- Try it without X: `go run ./cmd/mockx` (fake X on 127.0.0.1:8099, accounts `tok-a`, `tok-b`, `tok-c`), put those values in cookie files as `auth_token` (any `ct0`), and run the scraper with `X_BASE_URL=http://127.0.0.1:8099`.

## Still fragile

1. **Live x.com is untested** (see top). Likeliest first-run problems, in order: the query id in `x.operations` is stale, a feature flag is missing, or page size 100 is refused (set `followScrape.pageSize` to 20).
2. **X has a rebuilt web app.** Reading its public JavaScript (logged out) I found newer ids: `UserByScreenName` = `TjMgivMQwACkPkkOanGFRw`, and the Following list is now a query called `followingQuery` = `h8DgS5faBfBbLC6__tCW3g`, which takes `screenName`, `count`, `cursor` plus several `__relay_internal__pv__...` flags. It returns the list at `data.user_result_by_screen_name.result.following_timeline.timeline` (the parser already reads that layout). I did **not** switch to it, because I could not test the request format without a logged in session. If the old ids are dead on the first live run, this is where to look.
3. **Sessions.** Cookies expire and X flags automated sessions. Many accounts on one IP is the fastest way to lose them. `x.proxyUrl` is supported but untested.
4. **TLS fingerprint** is fixed at `Chrome_133_PSK` in `scrape/internals/api/client.go`; it ages as Chrome versions move on.
5. **"Complete" means X said there is no next page.** The stored count can be a little below the profile's number, because suspended or deleted accounts are not listed. The result line shows both numbers.
6. **The old pipeline code is still in the repo**, just off. `UserByRestId` and the other old calls still use hardcoded ids.

## Found while working (not changed)

- `config/env/app-development.yaml` contains a **production MongoDB URL with a password**, and it is the profile used when `ENVIRONMENT` is unset, so running the old service locally points it at production. `config/env/app.yaml` also has a bearer token. These should be rotated and moved to environment variables. The new tool avoids this by defaulting to the `local` profile and refusing remote databases.
- On start the service runs `ResetAll`, which marks every pooled account as free. That is harmless for one process but not if two run at once.
