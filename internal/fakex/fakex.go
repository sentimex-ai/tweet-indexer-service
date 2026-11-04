// Package fakex is an in-process imitation of the two x.com GraphQL
// endpoints the following scraper uses, for tests. Response shapes follow
// real 2025 x.com payloads (core.* user fields, user-/cursor-bottom- entry
// ids, "0|" end cursor, X error envelopes).
package fakex

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Profile struct {
	ID        string
	Handle    string
	Name      string
	Following int // size of generated following list
	Protected bool
	Suspended bool
}

type Server struct {
	*httptest.Server

	mu       sync.Mutex
	profiles map[string]*Profile // by lower handle
	byID     map[string]*Profile

	UserQueryID      string
	FollowingQueryID string
	RequiredFeature  string // if set, Following 400s (code 336) unless sent

	// Per-session behaviour, keyed by auth_token cookie value.
	RateLimitAfter map[string]int  // 429 after N successful calls
	Expired        map[string]bool // 401 code 32
	Suspended      map[string]bool // 403 code 64
	FailNext503    int             // next N calls return 503

	calls     map[string]int
	Requests  int
	SeenCSRF  []string
	MaxPage   int
	LastPaths []string
}

func New() *Server {
	s := &Server{
		profiles:         map[string]*Profile{},
		byID:             map[string]*Profile{},
		UserQueryID:      "uq",
		FollowingQueryID: "fq",
		RateLimitAfter:   map[string]int{},
		Expired:          map[string]bool{},
		Suspended:        map[string]bool{},
		calls:            map[string]int{},
		MaxPage:          50,
	}
	s.Server = httptest.NewServer(http.HandlerFunc(s.handle))
	return s
}

func (s *Server) Add(p Profile) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if p.Name == "" {
		p.Name = strings.ToUpper(p.Handle[:1]) + p.Handle[1:]
	}
	s.profiles[strings.ToLower(p.Handle)] = &p
	s.byID[p.ID] = &p
}

// SetFollowing changes the size of a profile's list (simulate unfollows).
func (s *Server) SetFollowing(handle string, n int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.profiles[strings.ToLower(handle)].Following = n
}

// FollowedID is the deterministic id of the i-th account a profile follows.
func FollowedID(ownerID string, i int) string { return fmt.Sprintf("%s%06d", ownerID, i) }

func writeErr(w http.ResponseWriter, status, code int, msg string) {
	w.Header().Set("content-type", "application/json")
	w.WriteHeader(status)
	fmt.Fprintf(w, `{"errors":[{"code":%d,"message":%q}]}`, code, msg)
}

func (s *Server) handle(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.Requests++
	s.LastPaths = append(s.LastPaths, r.URL.Path)

	tok := ""
	if c, err := r.Cookie("auth_token"); err == nil {
		tok = c.Value
	}
	s.SeenCSRF = append(s.SeenCSRF, r.Header.Get("x-csrf-token"))
	if !strings.HasPrefix(r.Header.Get("authorization"), "Bearer ") {
		writeErr(w, 401, 215, "Bad Authentication data.")
		return
	}
	if s.FailNext503 > 0 {
		s.FailNext503--
		w.WriteHeader(503)
		return
	}
	if tok == "" || s.Expired[tok] {
		writeErr(w, 401, 32, "Could not authenticate you.")
		return
	}
	if s.Suspended[tok] {
		writeErr(w, 403, 64, "Your account is suspended and is not permitted to access this feature.")
		return
	}
	reset := time.Now().Add(2 * time.Second).Unix()
	if lim, ok := s.RateLimitAfter[tok]; ok && s.calls[tok] >= lim {
		w.Header().Set("x-rate-limit-limit", strconv.Itoa(lim))
		w.Header().Set("x-rate-limit-remaining", "0")
		w.Header().Set("x-rate-limit-reset", strconv.FormatInt(reset, 10))
		writeErr(w, 429, 88, "Rate limit exceeded")
		return
	}
	s.calls[tok]++
	w.Header().Set("x-rate-limit-limit", "500")
	rem := 500 - s.calls[tok]
	if lim, ok := s.RateLimitAfter[tok]; ok {
		rem = lim - s.calls[tok]
	}
	w.Header().Set("x-rate-limit-remaining", strconv.Itoa(rem))
	w.Header().Set("x-rate-limit-reset", strconv.FormatInt(reset, 10))

	parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/i/api/graphql/"), "/")
	if len(parts) != 2 {
		w.WriteHeader(404)
		return
	}
	qid, op := parts[0], parts[1]
	var vars map[string]any
	_ = json.Unmarshal([]byte(r.URL.Query().Get("variables")), &vars)

	switch {
	case op == "UserByScreenName" && qid == s.UserQueryID:
		s.userByScreenName(w, fmt.Sprint(vars["screen_name"]))
	case op == "Following" && qid == s.FollowingQueryID:
		if s.RequiredFeature != "" && !strings.Contains(r.URL.Query().Get("features"), `"`+s.RequiredFeature+`"`) {
			writeErr(w, 400, 336, "The following features cannot be null: "+s.RequiredFeature)
			return
		}
		cur, _ := vars["cursor"].(string)
		cnt := 20
		if f, ok := vars["count"].(float64); ok {
			cnt = int(f)
		}
		s.following(w, fmt.Sprint(vars["userId"]), cur, cnt)
	default:
		w.WriteHeader(404) // what X does for an unknown query id
	}
}

func userJSON(p Profile) map[string]any {
	return map[string]any{
		"__typename": "User", "id": "VXNlcjo" + p.ID, "rest_id": p.ID, "is_blue_verified": len(p.ID)%2 == 0,
		"core":    map[string]any{"created_at": "Tue Mar 21 20:50:14 +0000 2006", "name": p.Name, "screen_name": p.Handle},
		"avatar":  map[string]any{"image_url": "https://pbs.twimg.com/profile_images/" + p.ID + "/a_normal.jpg"},
		"privacy": map[string]any{"protected": p.Protected},
		"legacy": map[string]any{
			"followers_count": 1000 + len(p.ID)*7, "friends_count": p.Following,
			"description": "", "verified": false,
		},
	}
}

func (s *Server) userByScreenName(w http.ResponseWriter, handle string) {
	p, ok := s.profiles[strings.ToLower(handle)]
	w.Header().Set("content-type", "application/json")
	if !ok {
		fmt.Fprint(w, `{"data":{}}`)
		return
	}
	if p.Suspended {
		fmt.Fprint(w, `{"data":{"user":{"result":{"__typename":"UserUnavailable","reason":"Suspended"}}}}`)
		return
	}
	_ = json.NewEncoder(w).Encode(map[string]any{"data": map[string]any{"user": map[string]any{"result": userJSON(*p)}}})
}

func (s *Server) following(w http.ResponseWriter, userID, cursor string, count int) {
	p, ok := s.byID[userID]
	w.Header().Set("content-type", "application/json")
	if !ok {
		fmt.Fprint(w, `{"data":{}}`)
		return
	}
	if count > s.MaxPage {
		count = s.MaxPage
	}
	start := 0
	if cursor != "" {
		// real cursors look like "1846182828208571264|1985102375018070000"
		start, _ = strconv.Atoi(strings.SplitN(cursor, "|", 2)[1])
	}
	end := start + count
	if end > p.Following {
		end = p.Following
	}
	var entries []any
	for i := start; i < end; i++ {
		fp := Profile{ID: FollowedID(p.ID, i), Handle: fmt.Sprintf("acct_%s_%d", p.ID, i), Name: fmt.Sprintf("Account %d", i), Following: i % 300}
		// every 97th followed account is unavailable (suspended/deactivated)
		var res any = userJSON(fp)
		if i%97 == 96 {
			res = map[string]any{"__typename": "UserUnavailable", "reason": "Suspended"}
		}
		entries = append(entries, map[string]any{
			"entryId": "user-" + fp.ID, "sortIndex": strconv.Itoa(1e9 - i),
			"content": map[string]any{
				"entryType": "TimelineTimelineItem", "__typename": "TimelineTimelineItem",
				"itemContent": map[string]any{"itemType": "TimelineUser", "__typename": "TimelineUser",
					"user_results": map[string]any{"result": res}, "userDisplayType": "User"},
			},
		})
	}
	next := fmt.Sprintf("1846182828208571264|%d", end)
	if end >= p.Following {
		next = "0|1985102375018070000"
	}
	entries = append(entries,
		map[string]any{"entryId": "cursor-bottom-" + next, "sortIndex": "1", "content": map[string]any{
			"entryType": "TimelineTimelineCursor", "__typename": "TimelineTimelineCursor", "value": next, "cursorType": "Bottom"}},
		map[string]any{"entryId": "cursor-top-x", "sortIndex": "2", "content": map[string]any{
			"entryType": "TimelineTimelineCursor", "__typename": "TimelineTimelineCursor", "value": "-1|x", "cursorType": "Top"}},
	)
	_ = json.NewEncoder(w).Encode(map[string]any{"data": map[string]any{"user": map[string]any{"result": map[string]any{
		"__typename": "User",
		"timeline": map[string]any{"timeline": map[string]any{"instructions": []any{
			map[string]any{"type": "TimelineClearCache"},
			map[string]any{"type": "TimelineTerminateTimeline", "direction": "Top"},
			map[string]any{"type": "TimelineAddEntries", "entries": entries},
		}}},
	}}}})
}

// Locked runs f while holding the server lock (for mutating behaviour maps
// from another goroutine).
func (s *Server) Locked(f func()) {
	s.mu.Lock()
	defer s.mu.Unlock()
	f()
}
