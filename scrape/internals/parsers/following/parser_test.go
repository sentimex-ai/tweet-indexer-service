package following

import (
	"testing"

	"github.com/tidwall/gjson"
)

func entry(result string) string {
	return `{"entryId":"user-42","content":{"itemContent":{"itemType":"TimelineUser","user_results":{"result":` + result + `}}}}`
}

func TestExtractPageLegacyLayoutCursorByTypeNotPosition(t *testing.T) {
	// cursor entries come FIRST here; the old code assumed they were last
	page := gjson.Parse(`{"data":{"user":{"result":{"timeline":{"timeline":{"instructions":[
	  {"type":"TimelineAddEntries","entries":[
	    {"entryId":"cursor-bottom-9","content":{"cursorType":"Bottom","value":"NEXT"}},
	    {"entryId":"cursor-top-9","content":{"cursorType":"Top","value":"TOP"}},
	    ` + entry(`{"rest_id":"1","legacy":{"screen_name":"a","name":"A"}}`) + `,
	    ` + entry(`{"rest_id":"2","legacy":{"screen_name":"b","name":"B"}}`) + `
	  ]}]}}}}}`)
	users, next := ExtractPage(&page)
	if len(users) != 2 || next != "NEXT" {
		t.Fatalf("users=%d next=%q", len(users), next)
	}
}

func TestExtractPageNewLayout(t *testing.T) {
	page := gjson.Parse(`{"data":{"user_result_by_screen_name":{"result":{"following_timeline":{"timeline":{"instructions":[
	  {"type":"TimelineAddEntries","entries":[` + entry(`{"rest_id":"7","core":{"screen_name":"c","name":"C"},"relationship_counts":{"followers":5,"following":6}}`) + `,
	    {"entryId":"cursor-bottom-1","content":{"cursorType":"Bottom","value":"0|123"}}]}]}}}}}}`)
	users, next := ExtractPage(&page)
	if len(users) != 1 || !IsEnd(next) {
		t.Fatalf("users=%d next=%q", len(users), next)
	}
	u := ParseFollowing(&users[0])
	if u == nil || u.ID != "7" || u.Handle != "c" || u.UserName != "C" || u.Followers != 5 || u.Following != 6 {
		t.Fatalf("parsed %+v", u)
	}
}

func TestParseFollowingLegacy(t *testing.T) {
	e := gjson.Parse(entry(`{"rest_id":"9","is_blue_verified":true,"legacy":{"screen_name":"h","name":"Display Name","followers_count":1200,"friends_count":34,"profile_image_url_https":"https://x/y.jpg"}}`))
	u := ParseFollowing(&e)
	if u == nil || u.ID != "9" || u.Handle != "h" || u.UserName != "Display Name" || u.Followers != 1200 || u.Following != 34 || !u.Verified {
		t.Fatalf("parsed %+v", u)
	}
}

func TestEmptyAndMalformedPagesAreSafe(t *testing.T) {
	for _, raw := range []string{`{}`, `{"data":{}}`, `{"errors":[{"code":88}]}`, `not json`} {
		r := gjson.Parse(raw)
		if users, next := ExtractPage(&r); len(users) != 0 || next != "" {
			t.Errorf("%q -> %d users, next %q", raw, len(users), next)
		}
	}
	if ParseFollowing(nil) != nil {
		t.Error("nil entry should parse to nil")
	}
}
