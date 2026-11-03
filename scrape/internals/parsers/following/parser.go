package following

import (
	"strings"

	twitterTypes "github.com/jeetdotso/tweet-indexer-service/scrape/internals/api/types"
	"github.com/tidwall/gjson"
)

// timelinePaths are the places X has put the following timeline. The first is
// what the web app returned when this service was written; the second is the
// newer user_result_by_screen_name layout.
var timelinePaths = []string{
	"data.user.result.timeline.timeline.instructions",
	"data.user.result.timeline_v2.timeline.instructions",
	"data.user_result_by_screen_name.result.following_timeline.timeline.instructions",
}

// ExtractPage returns the user entries of one Following page and the cursor for
// the next page ("" when there is none). Entries are recognised by what they
// are (a "user-" entry id or a TimelineUser item) and the cursor by its type,
// not by position, so a layout change does not silently drop or duplicate rows.
func ExtractPage(r *gjson.Result) (users []gjson.Result, nextCursor string) {
	if r == nil {
		return nil, ""
	}
	for _, p := range timelinePaths {
		for _, ins := range r.Get(p).Array() {
			switch ins.Get("type").String() {
			case "TimelineAddEntries":
				for _, e := range ins.Get("entries").Array() {
					id := e.Get("entryId").String()
					switch {
					case strings.HasPrefix(id, "user-") || e.Get("content.itemContent.itemType").String() == "TimelineUser":
						users = append(users, e)
					case strings.HasPrefix(id, "cursor-bottom") || e.Get("content.cursorType").String() == "Bottom":
						nextCursor = e.Get("content.value").String()
					}
				}
			case "TimelineReplaceEntry":
				if e := ins.Get("entry"); strings.HasPrefix(e.Get("entryId").String(), "cursor-bottom") {
					nextCursor = e.Get("content.value").String()
				}
			}
		}
		if len(users) > 0 || nextCursor != "" {
			return users, nextCursor
		}
	}
	return users, nextCursor
}

// IsEnd reports whether a bottom cursor means "no more pages".
func IsEnd(next string) bool {
	return next == "" || strings.HasPrefix(next, "0|")
}

func ParseFollowing(r *gjson.Result) *twitterTypes.FollowingUser {
	if r == nil {
		return nil
	}

	// 1) point at the inner "User" result
	node := r.Get("content.itemContent.user_results.result")
	if !node.Exists() {
		return nil
	}
	// 2) legacy holds the fields we care about; newer payloads move some to core
	legacy := node.Get("legacy")
	core := node.Get("core")
	if !legacy.Exists() && !core.Exists() {
		return nil
	}

	first := func(vals ...gjson.Result) gjson.Result {
		for _, v := range vals {
			if v.Exists() && v.String() != "" {
				return v
			}
		}
		return gjson.Result{}
	}

	// 3) build your struct
	restID := node.Get("rest_id").String()
	return &twitterTypes.FollowingUser{
		// use the Twitter REST ID as your Mongo _id
		ID: restID,
		// display name and handle
		UserName: first(legacy.Get("name"), core.Get("name")).String(),
		Handle:   first(legacy.Get("screen_name"), core.Get("screen_name")).String(),
		// profile image
		ProfileImg: first(legacy.Get("profile_image_url_https"), node.Get("avatar.image_url")).String(),
		// counts
		Followers: int(first(legacy.Get("followers_count"), node.Get("relationship_counts.followers")).Int()),
		Following: int(first(legacy.Get("friends_count"), node.Get("relationship_counts.following")).Int()),
		// verified (boolean in legacy)
		Verified: node.Get("is_blue_verified").Bool(),
	}
}
