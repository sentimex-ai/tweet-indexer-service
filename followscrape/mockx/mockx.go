// Package mockx is a small fake of the two x.com GraphQL calls the following
// scraper uses (UserByScreenName and Following). It exists so the scraper can be
// run end to end, including rate limits and dead sessions, without a real X
// account. It is test and demo support only; it never talks to the network.
package mockx

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

// Profile is a fake X user with a following list.
type Profile struct {
	Handle    string
	ID        string
	Name      string
	Protected bool
	Following int // how many accounts it follows; ids are "<ID>00001"...
	Suspended bool
}

// Config shapes the fake's behaviour.
type Config struct {
	Profiles []Profile
	// ValidTokens are the auth_token cookie values that count as logged in.
	ValidTokens []string
	// LimitPerWindow: after this many Following calls a token gets HTTP 429 until
	// the window resets (WindowSeconds). 0 = unlimited.
	LimitPerWindow int
	WindowSeconds  int
	// DieAfter: a token's session starts failing with code 32 after this many calls (0 = never).
	DieAfter map[string]int
	// FailEveryNth: every nth Following call answers 503 (0 = never).
	FailEveryNth int
	// StaleQueryIDs: answer 404 to any query id not in this set (empty = accept all).
	AcceptedQueryIDs []string
}

type Server struct {
	*httptest.Server
	mu    sync.Mutex
	cfg   Config
	calls map[string]int       // token -> Following calls
	win   map[string]time.Time // token -> window start
	total int
	Log   []string // one line per request, tokens masked
}

func New(cfg Config) *Server {
	s := &Server{cfg: cfg, calls: map[string]int{}, win: map[string]time.Time{}}
	s.Server = httptest.NewServer(http.HandlerFunc(s.handle))
	return s
}

// Calls returns how many Following calls a token made.
func (s *Server) Calls(token string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls[token]
}

func token(r *http.Request) string {
	c, err := r.Cookie("auth_token")
	if err != nil {
		return ""
	}
	return c.Value
}

func mask(t string) string {
	if len(t) <= 4 {
		return "****"
	}
	return t[:2] + "…" + t[len(t)-2:]
}

func write(w http.ResponseWriter, status int, body interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func apiErr(code int, msg string) map[string]interface{} {
	return map[string]interface{}{"errors": []map[string]interface{}{{"code": code, "message": msg}}}
}

func (s *Server) handle(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()
	tok := token(r)
	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/") // i api graphql <id> <name>
	if len(parts) < 5 || parts[2] != "graphql" {
		write(w, 404, apiErr(34, "Sorry, that page does not exist"))
		return
	}
	qid, op := parts[3], parts[4]
	s.Log = append(s.Log, fmt.Sprintf("%s %s token=%s", op, r.URL.Query().Get("variables"), mask(tok)))

	if len(s.cfg.AcceptedQueryIDs) > 0 {
		ok := false
		for _, a := range s.cfg.AcceptedQueryIDs {
			ok = ok || a == qid
		}
		if !ok {
			write(w, 404, map[string]interface{}{"errors": []map[string]interface{}{{"message": "Not Found"}}})
			return
		}
	}
	valid := false
	for _, v := range s.cfg.ValidTokens {
		valid = valid || v == tok
	}
	if !valid {
		write(w, 401, apiErr(32, "Could not authenticate you"))
		return
	}
	if lim, ok := s.cfg.DieAfter[tok]; ok && lim > 0 && s.calls[tok] >= lim {
		write(w, 401, apiErr(32, "Could not authenticate you"))
		return
	}

	switch op {
	case "UserByScreenName":
		var v struct {
			ScreenName string `json:"screen_name"`
		}
		_ = json.Unmarshal([]byte(r.URL.Query().Get("variables")), &v)
		p := s.find(v.ScreenName)
		if p == nil {
			write(w, 200, map[string]interface{}{"data": map[string]interface{}{}})
			return
		}
		if p.Suspended {
			write(w, 200, map[string]interface{}{"data": map[string]interface{}{"user": map[string]interface{}{"result": map[string]interface{}{"__typename": "UserUnavailable", "reason": "Suspended"}}}})
			return
		}
		write(w, 200, map[string]interface{}{"data": map[string]interface{}{"user": map[string]interface{}{"result": map[string]interface{}{
			"__typename": "User", "rest_id": p.ID,
			"legacy": map[string]interface{}{"screen_name": p.Handle, "name": p.Name, "friends_count": p.Following, "protected": p.Protected, "following": false},
		}}}})
	case "Following":
		s.calls[tok]++
		s.total++
		if s.cfg.FailEveryNth > 0 && s.total%s.cfg.FailEveryNth == 0 {
			write(w, 503, map[string]string{"error": "service unavailable"})
			return
		}
		if s.cfg.LimitPerWindow > 0 {
			win := time.Duration(s.cfg.WindowSeconds) * time.Second
			start, seen := s.win[tok]
			if !seen || time.Since(start) >= win {
				s.win[tok], start = time.Now(), time.Now()
				s.calls[tok] = 1
			}
			if s.calls[tok] > s.cfg.LimitPerWindow {
				w.Header().Set("x-rate-limit-reset", strconv.FormatInt(start.Add(win).Unix(), 10))
				write(w, 429, apiErr(88, "Rate limit exceeded"))
				return
			}
		}
		var v struct {
			UserID string `json:"userId"`
			Count  int    `json:"count"`
			Cursor string `json:"cursor"`
		}
		_ = json.Unmarshal([]byte(r.URL.Query().Get("variables")), &v)
		var p *Profile
		for i := range s.cfg.Profiles {
			if s.cfg.Profiles[i].ID == v.UserID {
				p = &s.cfg.Profiles[i]
			}
		}
		if p == nil {
			write(w, 200, apiErr(50, "User not found."))
			return
		}
		write(w, 200, Page(p.ID, p.Following, v.Cursor, v.Count))
	default:
		write(w, 404, apiErr(34, "unknown operation"))
	}
}

func (s *Server) find(handle string) *Profile {
	for i := range s.cfg.Profiles {
		if strings.EqualFold(s.cfg.Profiles[i].Handle, handle) {
			return &s.cfg.Profiles[i]
		}
	}
	return nil
}

// FollowedID is the id of the i-th (1-based) account profile id follows.
func FollowedID(profileID string, i int) string { return fmt.Sprintf("%s%05d", profileID, i) }

// Page builds the GraphQL answer for one page of profile's following list, in
// the layout x.com returns: a TimelineAddEntries instruction with one entry per
// user followed by a bottom cursor and a top cursor.
func Page(profileID string, total int, cursor string, count int) map[string]interface{} {
	if count <= 0 || count > 100 {
		count = 20
	}
	start := 0
	if strings.HasPrefix(cursor, "c") {
		start, _ = strconv.Atoi(cursor[1:])
	}
	end := start + count
	if end > total {
		end = total
	}
	entries := []map[string]interface{}{}
	for i := start + 1; i <= end; i++ {
		id := FollowedID(profileID, i)
		entries = append(entries, map[string]interface{}{
			"entryId": "user-" + id,
			"content": map[string]interface{}{"entryType": "TimelineTimelineItem", "itemContent": map[string]interface{}{
				"itemType": "TimelineUser",
				"user_results": map[string]interface{}{"result": map[string]interface{}{
					"__typename": "User", "rest_id": id, "is_blue_verified": i%7 == 0,
					"legacy": map[string]interface{}{
						"screen_name": "user" + id, "name": "Name " + id, "profile_image_url_https": "https://example.invalid/" + id + ".jpg",
						"followers_count": 1000 + i, "friends_count": 100 + i,
					},
				}},
			}},
		})
	}
	next := fmt.Sprintf("c%d", end)
	if end >= total {
		next = "0|0"
	}
	entries = append(entries,
		map[string]interface{}{"entryId": "cursor-top-1", "content": map[string]interface{}{"entryType": "TimelineTimelineCursor", "cursorType": "Top", "value": "0|top"}},
		map[string]interface{}{"entryId": "cursor-bottom-1", "content": map[string]interface{}{"entryType": "TimelineTimelineCursor", "cursorType": "Bottom", "value": next}},
	)
	return map[string]interface{}{"data": map[string]interface{}{"user": map[string]interface{}{"result": map[string]interface{}{
		"timeline": map[string]interface{}{"timeline": map[string]interface{}{"instructions": []map[string]interface{}{
			{"type": "TimelineClearCache"},
			{"type": "TimelineAddEntries", "entries": entries},
		}}},
	}}}}
}

// CallsSnapshot is a copy of the per-token Following call counts.
func (s *Server) CallsSnapshot() map[string]int {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := map[string]int{}
	for k, v := range s.calls {
		out[k] = v
	}
	return out
}
