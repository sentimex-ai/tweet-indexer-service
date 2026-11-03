package twitterAPI

import (
	"bytes"
	"fmt"
	"io"

	http "github.com/bogdanfinn/fhttp"
	tls_client "github.com/bogdanfinn/tls-client"
	profiles "github.com/bogdanfinn/tls-client/profiles"

	utils "github.com/jeetdotso/tweet-indexer-service/utils"
)

type TwitterClient struct {
	BaseURL string
	Client  *tls_client.HttpClient
	Headers http.Header
	Cookies []*http.Cookie
}

type NewTwitterClientOptions struct {
	BaseURL       string
	CustomHeaders http.Header
	Cookies       []*http.Cookie
	ProxyURL      string
}

func NewTwitterClient(opts *NewTwitterClientOptions) (*TwitterClient, error) {
	if opts.BaseURL == "" {
		opts.BaseURL = "https://x.com"
	}
	options := []tls_client.HttpClientOption{
		tls_client.WithTimeoutSeconds(30),
		tls_client.WithClientProfile(profiles.Chrome_133_PSK),
		tls_client.WithNotFollowRedirects(),
	}
	if opts.ProxyURL != "" {
		options = append(options, tls_client.WithProxyUrl(opts.ProxyURL))
	}
	client, err := tls_client.NewHttpClient(tls_client.NewNoopLogger(), options...)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize TLS client: %w", err)
	}
	headers := http.Header{
		"authority":                 {"x.com"},
		"accept":                    {"*/*"},
		"accept-language":           {"en-US,en;q=0.9"},
		"authorization":             {"Bearer AAAAAAAAAAAAAAAAAAAAANRILgAAAAAAnNwIzUejRCOuH5E6I8xnZz4puTs%3D1Zv7ttfk8LF81IUq16cHjhLTvJu4FA33AGWWjCpTnA"},
		"Host":                      {"x.com"},
		"origin":                    {"https://x.com"},
		"referer":                   {"https://x.com/"},
		"Content-Type":              {"application/json"},
		"sec-fetch-dest":            {"empty"},
		"sec-fetch-mode":            {"cors"},
		"sec-fetch-site":            {"same-origin"},
		"sec-gpc":                   {"1"},
		"user-agent":                {"Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:133.0) Gecko/20100101 Firefox/133.0"},
		"x-twitter-active-user":     {"yes"},
		"x-twitter-auth-type":       {"OAuth2Session"},
		"x-twitter-client-language": {"en"},
	}
	headers["x-csrf-token"] = []string{*utils.GetCookieByName(&opts.Cookies, "ct0")}
	if opts.CustomHeaders != nil {
		for k, v := range opts.CustomHeaders {
			headers[k] = v
		}
	}
	return &TwitterClient{
		BaseURL: opts.BaseURL,
		Client:  &client,
		Headers: headers,
		Cookies: opts.Cookies,
	}, nil
}

func (tc *TwitterClient) MakeRequest(method, url string, body io.Reader, headers *http.Header) (*[]byte, int, error) {
	b, status, _, err := tc.makeRequest(method, url, body, headers)
	return b, status, err
}

func (tc *TwitterClient) getWithHeaders(url string, headers *http.Header) (*[]byte, int, http.Header, error) {
	return tc.makeRequest(http.MethodGet, url, nil, headers)
}

func (tc *TwitterClient) makeRequest(method, url string, body io.Reader, headers *http.Header) (*[]byte, int, http.Header, error) {
	// Clone the body so it can be read and re-used
	var bodyBytes []byte
	if body != nil {
		var err error
		bodyBytes, err = io.ReadAll(body)
		if err != nil {
			return nil, 0, nil, fmt.Errorf("failed to read request body: %w", err)
		}
		body = bytes.NewReader(bodyBytes) // Reset body reader after reading
	}

	req, err := http.NewRequest(method, url, body)
	if err != nil {
		return nil, 0, nil, fmt.Errorf("failed to create %s request: %w", method, err)
	}

	// Clone and optionally override headers
	req.Header = tc.Headers.Clone()
	if headers != nil {
		for k, v := range *headers {
			req.Header[k] = v
		}
	}

	// Attach cookies
	for _, cookie := range tc.Cookies {
		req.AddCookie(cookie)
	}

	// Perform request
	resp, err := (*tc.Client).Do(req)
	if err != nil {
		return nil, 0, nil, fmt.Errorf("%s request failed", method)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, resp.StatusCode, resp.Header, fmt.Errorf("failed to read response body: %w", err)
	}

	return &respBody, resp.StatusCode, resp.Header, nil
}

func (tc *TwitterClient) Get(url string, headers *http.Header) (*[]byte, int, error) {
	return tc.MakeRequest(http.MethodGet, url, nil, headers)
}
func (tc *TwitterClient) Get2(url string, body io.Reader, headers *http.Header) (*[]byte, int, error) {
	return tc.MakeRequest(http.MethodGet, url, body, headers)
}

func (tc *TwitterClient) Post(url string, body io.Reader, headers *http.Header) (*[]byte, int, error) {
	return tc.MakeRequest(http.MethodPost, url, body, headers)
}

func CreateTwitterClient() (*TwitterClient, error) {
	cookiesPool := utils.BuildCookiesPool()
	return NewTwitterClient(&NewTwitterClientOptions{
		BaseURL: "https://x.com",
		Cookies: *cookiesPool.GetRandomCookies(),
	})
}

func CreateTwitterClientWithCookies(cookies []*http.Cookie, proxy string) (*TwitterClient, error) {
	return NewTwitterClient(&NewTwitterClientOptions{
		BaseURL:  "https://x.com",
		Cookies:  cookies,
		ProxyURL: proxy,
	})
}
