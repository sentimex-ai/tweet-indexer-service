package twitterAPI

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"

	"github.com/tidwall/gjson"
)

type GrokClient struct {
	BaseURL    string
	AuthToken  string
	Ct0        string
	HTTPClient *http.Client
}

func NewGrokClient(authToken, ct0 string) *GrokClient {
	return &GrokClient{
		BaseURL:    "http://localhost:8000",
		AuthToken:  authToken,
		Ct0:        ct0,
		HTTPClient: &http.Client{},
	}
}

func (c *GrokClient) SetBaseURL(url string) {
	c.BaseURL = url
}

type Response struct {
	Success bool            `json:"success"`
	Data    json.RawMessage `json:"data,omitempty"`
	Error   string          `json:"error,omitempty"`
}

// CreateConversation creates a new conversation (Not required to start conv)
func (c *GrokClient) CreateConversation(proxy string) (string, error) {
	reqBody := struct {
		AuthToken string `json:"auth_token"`
		Ct0       string `json:"ct0"`
		Proxy     string `json:"proxy,omitempty"`
	}{
		AuthToken: c.AuthToken,
		Ct0:       c.Ct0,
		Proxy:     proxy,
	}

	body, err := c.doRequest("POST", "/create_conversation", reqBody)
	if err != nil {
		return "", err
	}

	// Use gjson to parse the response
	result := gjson.ParseBytes(body)
	if !result.Get("success").Bool() {
		return "", errors.New(result.Get("error").String())
	}

	// Try to get conversation_id from both possible locations
	convID := result.Get("data.conversation_id").String()
	if convID == "" {
		convID = result.Get("data.data.create_grok_conversation.conversation_id").String()
	}

	if convID == "" {
		return "", errors.New("conversation_id not found in response")
	}

	return convID, nil
}


// SendMessage sends a message to Grok (you can use this request only and get the response in one go)
func (c *GrokClient) SendMessage(message, conversationID, model, proxy string, rawResponse bool) (string, string, error) {
	reqBody := struct {
		AuthToken      string `json:"auth_token"`
		Ct0            string `json:"ct0"`
		Message        string `json:"message"`
		ConversationID string `json:"conversation_id,omitempty"`
		Model          string `json:"model,omitempty"`
		Proxy          string `json:"proxy,omitempty"`
		RawResponse    bool   `json:"raw_response,omitempty"`
	}{
		AuthToken:      c.AuthToken,
		Ct0:            c.Ct0,
		Message:        message,
		ConversationID: conversationID,
		Model:          model,
		Proxy:          proxy,
		RawResponse:    rawResponse,
	}

	body, err := c.doRequest("POST", "/send_message", reqBody)
	if err != nil {
		return "", "", err
	}

	result := gjson.ParseBytes(body)
	if !result.Get("success").Bool() {
		return "", "", errors.New(result.Get("error").String())
	}

	cleanResponse := result.Get("data.clean_response").String()
	rawResp := result.Get("data.raw_response").String()

	return cleanResponse, rawResp, nil
}

// AnalyzeTweet sends a tweet link to the analyze-tweet endpoint and returns Grok's response
func (c *GrokClient) AnalyzeTweet(tweetLink, conversationID, model, proxy string, rawResponse bool) (string, string, error) {
	reqBody := struct {
		AuthToken      string `json:"auth_token"`
		Ct0            string `json:"ct0"`
		Message        string `json:"message"` // tweetLink is passed as "message"
		ConversationID string `json:"conversation_id,omitempty"`
		Model          string `json:"model,omitempty"`
		Proxy          string `json:"proxy,omitempty"`
		RawResponse    bool   `json:"raw_response,omitempty"`
	}{
		AuthToken:      c.AuthToken,
		Ct0:            c.Ct0,
		Message:        tweetLink,
		ConversationID: conversationID,
		Model:          model,
		Proxy:          proxy,
		RawResponse:    rawResponse,
	}

	body, err := c.doRequest("POST", "/analyze-tweet", reqBody)
	if err != nil {
		return "", "", err
	}

	result := gjson.ParseBytes(body)
	if !result.Get("success").Bool() {
		return "", "", errors.New(result.Get("error").String())
	}

	cleanResponse := result.Get("data.clean_response").String()
	rawResp := result.Get("data.raw_response").String()

	return cleanResponse, rawResp, nil
}

// GetConversationHistory retrieves the message history for a conversation (this is not requirede or needed just added it, maybe for further use)
func (c *GrokClient) GetConversationHistory(conversationID, proxy string) (string, error) {
	url := fmt.Sprintf("%s/conversation_history?auth_token=%s&ct0=%s&conversation_id=%s", 
		c.BaseURL, c.AuthToken, c.Ct0, conversationID)
	if proxy != "" {
		url += "&proxy=" + proxy
	}

	resp, err := c.HTTPClient.Get(url)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}

	result := gjson.ParseBytes(body)
	if !result.Get("success").Bool() {
		return "", errors.New(result.Get("error").String())
	}

	return result.Get("data").Raw, nil
}


// GenerateTransactionID generates an X-Client-Transaction ID (this is very important you can use it as util and et the x_transaction_id for any endpoint)
func (c *GrokClient) GenerateTransactionID(method, path, proxy string) (string, error) {
	url := fmt.Sprintf("%s/generate_transaction_id?auth_token=%s&ct0=%s&method=%s&path=%s", 
		c.BaseURL, c.AuthToken, c.Ct0, method, path)
	if proxy != "" {
		url += "&proxy=" + proxy
	}

	resp, err := c.HTTPClient.Get(url)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}

	result := gjson.ParseBytes(body)
	if !result.Get("success").Bool() {
		return "", errors.New(result.Get("error").String())
	}

	return result.Get("data.transaction_id").String(), nil
}

// doRequest is a helper method to perform HTTP requests
func (c *GrokClient) doRequest(method, path string, requestBody interface{}) ([]byte, error) {
	var body io.Reader
	if requestBody != nil {
		jsonData, err := json.Marshal(requestBody)
		if err != nil {
			return nil, err
		}
		body = bytes.NewBuffer(jsonData)
	}

	req, err := http.NewRequest(method, c.BaseURL+path, body)
	if err != nil {
		return nil, err
	}

	req.Header.Set("Content-Type", "application/json")

	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("API request failed with status %d", resp.StatusCode)
	}

	return io.ReadAll(resp.Body)
}
