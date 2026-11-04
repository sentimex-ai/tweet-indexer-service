package xclient

import (
	"context"
	"strings"

	"github.com/tidwall/gjson"
)

// User is the subset of an X user object we store.
type User struct {
	ID             string
	Handle         string
	DisplayName    string
	FollowerCount  int
	FollowingCount int
	Verified       bool
	Protected      bool
	ProfileImg     string
}

// ParseUser reads a GraphQL User result node. X moved name/screen_name from
// `legacy` to `core` (mid-2025) and the avatar to `avatar.image_url`; both
// layouts are accepted so a rollback on X's side does not break us.
func ParseUser(n gjson.Result) (User, bool) {
	if n.Get("__typename").String() == "UserWithVisibilityResults" {
		n = n.Get("user")
	}
	id := n.Get("rest_id").String()
	if id == "" || n.Get("__typename").String() == "UserUnavailable" {
		return User{}, false
	}
	u := User{
		ID:             id,
		Handle:         first(n, "core.screen_name", "legacy.screen_name"),
		DisplayName:    first(n, "core.name", "legacy.name"),
		FollowerCount:  int(n.Get("legacy.followers_count").Int()),
		FollowingCount: int(n.Get("legacy.friends_count").Int()),
		Verified:       n.Get("is_blue_verified").Bool() || n.Get("verification.verified").Bool() || n.Get("legacy.verified").Bool(),
		Protected:      n.Get("privacy.protected").Bool() || n.Get("legacy.protected").Bool(),
		ProfileImg:     first(n, "avatar.image_url", "legacy.profile_image_url_https"),
	}
	return u, u.Handle != ""
}

func first(n gjson.Result, paths ...string) string {
	for _, p := range paths {
		if v := n.Get(p).String(); v != "" {
			return v
		}
	}
	return ""
}

// UserByScreenName resolves a handle to a user.
func (c *Client) UserByScreenName(ctx context.Context, handle string) (User, RateInfo, error) {
	handle = NormalizeHandle(handle)
	body, ri, err := c.graphQL(ctx, OpUserByScreenName, map[string]any{"screen_name": handle}, c.base+"/"+handle)
	if err != nil {
		return User{}, ri, err
	}
	res := gjson.ParseBytes(body)
	node := res.Get("data.user.result")
	if !node.Exists() {
		return User{}, ri, &Error{Kind: KindTargetNotFound, Op: OpUserByScreenName, Status: 200, Msg: "no such user: " + handle}
	}
	if node.Get("__typename").String() == "UserUnavailable" {
		reason := node.Get("reason").String()
		kind := KindTargetNotFound
		if strings.EqualFold(reason, "Suspended") {
			kind = KindTargetSuspended
		}
		return User{}, ri, &Error{Kind: kind, Op: OpUserByScreenName, Status: 200, Msg: handle + " unavailable: " + reason}
	}
	u, ok := ParseUser(node)
	if !ok {
		return User{}, ri, &Error{Kind: KindStaleQuery, Op: OpUserByScreenName, Status: 200,
			Msg: "user result did not contain rest_id/screen_name; response shape changed"}
	}
	return u, ri, nil
}

// Page is one page of a Following timeline.
type Page struct {
	Users      []User
	Skipped    int    // entries that were unavailable/unparseable users
	NextCursor string // "" when the list is finished
}

// FollowingPage fetches one page. Pass cursor "" for the first page.
func (c *Client) FollowingPage(ctx context.Context, userID, handle, cursor string, count int) (Page, RateInfo, error) {
	vars := map[string]any{"userId": userID, "count": count}
	if cursor != "" {
		vars["cursor"] = cursor
	}
	body, ri, err := c.graphQL(ctx, OpFollowing, vars, c.base+"/"+NormalizeHandle(handle)+"/following")
	if err != nil {
		return Page{}, ri, err
	}
	p, perr := ParseFollowingPage(gjson.ParseBytes(body))
	if perr != nil {
		return Page{}, ri, perr
	}
	return p, ri, nil
}

// ParseFollowingPage extracts users and the bottom cursor from a Following
// response. Cursors are identified by entryId, not by position.
func ParseFollowingPage(res gjson.Result) (Page, error) {
	ur := res.Get("data.user.result")
	if !ur.Exists() {
		return Page{}, &Error{Kind: KindTargetNotFound, Op: OpFollowing, Status: 200, Msg: "no user in following response"}
	}
	instr := ur.Get("timeline.timeline.instructions")
	if !instr.Exists() {
		instr = ur.Get("timeline_v2.timeline.instructions")
	}
	if !instr.Exists() {
		if ur.Get("__typename").String() == "UserUnavailable" {
			return Page{}, &Error{Kind: KindTargetSuspended, Op: OpFollowing, Status: 200, Msg: "target unavailable"}
		}
		return Page{}, &Error{Kind: KindStaleQuery, Op: OpFollowing, Status: 200, Msg: "no timeline instructions; response shape changed"}
	}

	var p Page
	handleEntry := func(e gjson.Result) {
		id := e.Get("entryId").String()
		switch {
		case strings.HasPrefix(id, "cursor-bottom"):
			p.NextCursor = e.Get("content.value").String()
		case strings.HasPrefix(id, "cursor-"):
			// cursor-top etc.
		case strings.HasPrefix(id, "user-"):
			if u, ok := ParseUser(e.Get("content.itemContent.user_results.result")); ok {
				p.Users = append(p.Users, u)
			} else {
				p.Skipped++
			}
		}
	}
	for _, in := range instr.Array() {
		switch in.Get("type").String() {
		case "TimelineAddEntries":
			for _, e := range in.Get("entries").Array() {
				handleEntry(e)
			}
		case "TimelineReplaceEntry":
			handleEntry(in.Get("entry"))
		}
	}
	if IsEndCursor(p.NextCursor) {
		p.NextCursor = ""
	}
	return p, nil
}

// IsEndCursor: X signals the end of a list with an empty cursor or one whose
// first segment is 0 ("0|1234...").
func IsEndCursor(c string) bool {
	return c == "" || strings.HasPrefix(c, "0|")
}

// NormalizeHandle strips @, whitespace and a pasted x.com URL.
func NormalizeHandle(h string) string {
	h = strings.TrimSpace(h)
	for _, p := range []string{"https://", "http://", "www."} {
		h = strings.TrimPrefix(h, p)
	}
	for _, p := range []string{"x.com/", "twitter.com/"} {
		h = strings.TrimPrefix(h, p)
	}
	if i := strings.IndexAny(h, "/?#"); i >= 0 {
		h = h[:i]
	}
	return strings.TrimPrefix(h, "@")
}
