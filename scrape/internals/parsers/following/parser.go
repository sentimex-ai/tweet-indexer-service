package following

import (
	twitterTypes "github.com/jeetdotso/tweet-indexer-service/scrape/internals/api/types"
	"github.com/jeetdotso/tweet-indexer-service/scrape/xclient"
	"github.com/tidwall/gjson"
)

// ParseFollowing parses one Following timeline entry.
//
// X moved screen_name/name from `legacy` to `core` in 2025; the old version of
// this function only read `legacy.*`, so every entry came back with an empty
// handle and was dropped. It now delegates to xclient.ParseUser, which reads
// both layouts.
func ParseFollowing(r *gjson.Result) *twitterTypes.FollowingUser {
	if r == nil {
		return nil
	}
	u, ok := xclient.ParseUser(r.Get("content.itemContent.user_results.result"))
	if !ok {
		return nil
	}
	return &twitterTypes.FollowingUser{
		ID:         u.ID,
		UserName:   u.DisplayName,
		Handle:     u.Handle,
		ProfileImg: u.ProfileImg,
		Followers:  u.FollowerCount,
		Following:  u.FollowingCount,
		Verified:   u.Verified,
	}
}
