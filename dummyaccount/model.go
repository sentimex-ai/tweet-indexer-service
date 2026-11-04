package dummyaccount

import (
	"errors"
	"net/http"
	"time"

	fhttp "github.com/bogdanfinn/fhttp"
	"github.com/tebeka/selenium"
)

var (
	errEmptyAccount  = errors.New("error empty selenium account")
	errCookiesFailed = errors.New("error using cookies failed")
	ErrUserNotFound  = errors.New("error user not found")
)

type SelAccount struct {
	UserName     string            `bson:"username" json:"username"`
	Password     string            `bson:"password" json:"password"`
	BoughtFrom   string            `bson:"bought_from" json:"bought_from"`
	Group        int               `bson:"group" json:"group"`
	LastLoggedIn time.Time         `bson:"last_log_in" json:"last_log_in"`
	LastGrok     time.Time         `bson:"last_grok" json:"last_grok"`
	InUse        bool              `bson:"in_use" json:"in_use"`
	InGrokUse    bool              `bson:"in_grok_use" json:"in_grok_use"`
	Cookies      []selenium.Cookie `bson:"cookies" json:"cookies"`
	TwoFAPre     string            `bson:"two_fa_pre" json:"two_fa_pre"`
	TwoFACode    string            `bson:"two_fa_code" json:"two_fa_code"`
	UpdatedAt    time.Time         `bson:"updated_at" json:"updated_at"`
	// Set by the following scraper: account is skipped until this time.
	RateLimitedUntil time.Time `bson:"rate_limited_until" json:"rate_limited_until"`
	// Last failure kind seen by the scraper (session_expired, account_suspended, ...).
	LastError   string    `bson:"last_error" json:"last_error"`
	LastErrorAt time.Time `bson:"last_error_at" json:"last_error_at"`
}

func (sA *SelAccount) GetID() any {
	return sA.UserName
}

func NewAccount(username, password string, group int, from string) *SelAccount {
	return &SelAccount{
		UserName:     username,
		Password:     password,
		InUse:        false,
		InGrokUse:    false,
		LastLoggedIn: time.Now(),
		LastGrok:     time.Now(),
		Cookies:      []selenium.Cookie{},
		Group:        group,
		UpdatedAt:    time.Now(),
		BoughtFrom:   from,
	}
}
func NewTwoFaAccount(username, password string, group int, twoFaPre, from string) *SelAccount {
	return &SelAccount{
		UserName:     username,
		Password:     password,
		InUse:        false,
		InGrokUse:    false,
		LastLoggedIn: time.Now(),
		LastGrok:     time.Now(),
		Cookies:      []selenium.Cookie{},
		TwoFAPre:     twoFaPre,
		Group:        group,
		UpdatedAt:    time.Now(),
		BoughtFrom:   from,
	}
}

func (sc *SelAccount) AddSeleniumCookies(cookies []selenium.Cookie) {
	sc.Cookies = append(sc.Cookies, cookies...)
	sc.UpdatedAt = time.Now()
	sc.LastLoggedIn = time.Now()
}

func (sc *SelAccount) LoggInGood() bool {
	if sc.Cookies == nil || len(sc.Cookies) == 0 {
		return false
	}
	return true
}

type Tweet struct {
	ID              string    `json:"_id"`
	CreatedAt       time.Time `json:"created_at"`
	TweetText       string    `json:"text"`
	AttachedImages  []string  `json:"attached_images"`
	AttachedLinks   []string  `json:"attached_links"`
	RetweetUserName string    `json:"retweeted_username"`
	Video           Video     `json:"video"`
	Quote           Quote     `json:"quote"`
}
type Video struct {
	Poster string `json:"poster"`
	Source string `json:"source"`
}
type Quote struct {
	UserName string `json:"userName"`
	Text     string `json:"text"`
}

func (sc *SelAccount) GetCookies() []selenium.Cookie {
	return sc.Cookies
}
func (sc *SelAccount) GetCookiesHttp() []*fhttp.Cookie {
	cookies := make([]*fhttp.Cookie, 0)
	for _, c := range sc.Cookies {
		cookies = append(cookies, &fhttp.Cookie{
			Name:       c.Name,
			Value:      c.Value,
			Path:       c.Path,
			Domain:     c.Domain,
			Expires:    time.Unix(int64(c.Expiry), 0),
			RawExpires: time.Unix(int64(c.Expiry), 0).Format(time.RFC1123),
			Secure:     c.Secure,
		})
	}
	return cookies
}

func (sc *SelAccount) GetCookiesRealHttp() []*http.Cookie {
	cookies := make([]*http.Cookie, 0)
	for _, c := range sc.Cookies {
		cookies = append(cookies, &http.Cookie{
			Name:  c.Name,
			Value: c.Value,
		})
	}
	return cookies
}
