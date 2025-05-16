package twitterAPI

import (
	"bytes"
	"encoding/json"
	"fmt"
	"time"

	"github.com/tidwall/gjson"
	http "github.com/bogdanfinn/fhttp"
)

const (
	grokCreateConvEndpoint = "/i/api/graphql/vvC5uy7pWWHXS2aDi1FZeA/CreateGrokConversation"
	grokAddResponseURL     = "https://grok.x.com/2/grok/add_response.json"
	grokGetConvEndpoint   = "/i/api/graphql/y1Lkc52sobXGzJ8ADjNEIw/GrokConversationItemsByRestId"
	maxRetries            = 3
	baseRetryDelay        = 2 * time.Second
)

var (
	HardcodedCookies = []*http.Cookie{
		{Name: "auth_token", Value: "da110b5c6b681e1540689ea828594dc575c31836"},
		{Name: "ct0", Value: "5d338a6ca6edb9711f59f80e469edd6bef26ad9ab2972495d0b01da2cafb2c35f536725e804b4dc084d9dc8d810079613970bfe5e531a549a243661a3f0d0948a6e86d85035fb2ad0358a1d7f8f008ee"},
	}
)

type GrokClient struct {
	twitterClient *TwitterClient
}

func NewGrokClient(twitterClient *TwitterClient) *GrokClient {
	return &GrokClient{
		twitterClient: twitterClient,
	}
}

type GrokResponse struct {
	ConversationID string
	Messages       []GrokMessage
}

type GrokMessage struct {
	SenderType string
	Message    string
	Timestamp  time.Time
}

type GrokRequest struct {
	ConversationID       string                 `json:"conversationId"`
	EnableSideBySide     bool                   `json:"enableSideBySide"`
	GrokModelOptionID    string                 `json:"grokModelOptionId"`
	ImageGenerationCount int                    `json:"imageGenerationCount"`
	PromptMetadata       map[string]interface{} `json:"promptMetadata"`
	RequestFeatures      map[string]bool        `json:"requestFeatures"`
	Responses            []GrokRequestMessage   `json:"responses"`
	ReturnCitations      bool                   `json:"returnCitations"`
	ReturnSearchResults  bool                   `json:"returnSearchResults"`
	SystemPromptName     string                 `json:"systemPromptName"`
	ToolOverrides        map[string]interface{} `json:"toolOverrides"`
	IsDeepsearch         bool                   `json:"isDeepsearch"`
	IsReasoning          bool                   `json:"isReasoning"`
}

type GrokRequestMessage struct {
	Message         string        `json:"message"`
	Sender          int           `json:"sender"`
	PromptSource    string        `json:"promptSource,omitempty"`
	FileAttachments []interface{} `json:"fileAttachments"`
}

// CreateConversation creates a new Grok conversation with retry logic
func (gc *GrokClient) CreateConversation() (string, error) {
	payload := map[string]interface{}{
		"variables": map[string]interface{}{},
		"queryId":   "vvC5uy7pWWHXS2aDi1FZeA",
	}

	var conversationID string
	var lastErr error

	for attempt := 0; attempt < maxRetries; attempt++ {
		if attempt > 0 {
			time.Sleep(1 * time.Second)
		}

		bodyBytes, err := json.Marshal(payload)
		if err != nil {
			lastErr = fmt.Errorf("failed to marshal payload: %w", err)
			continue
		}

		respBody, statusCode, err := gc.twitterClient.Post(
			gc.twitterClient.BaseURL+grokCreateConvEndpoint,
			bytes.NewReader(bodyBytes),
			nil,
		)
		if err != nil {
			lastErr = fmt.Errorf("request failed: %w", err)
			continue
		}

		if statusCode != http.StatusOK {
			lastErr = fmt.Errorf("unexpected status code: %d", statusCode)
			continue
		}

		result := gjson.ParseBytes(*respBody)
		conversationID = result.Get("data.create_grok_conversation.conversation_id").String()
		if conversationID == "" {
			lastErr = fmt.Errorf("empty conversation ID in response")
			continue
		}

		return conversationID, nil
	}

	return "", fmt.Errorf("failed after %d attempts: %w", maxRetries, lastErr)
}

// SendPrompt sends a message to Grok and returns the response
func (gc *GrokClient) SendPrompt(conversationID, message string) (*GrokResponse, error) {
	// Prepare the prompt request
	req := GrokRequest{
		ConversationID:       conversationID,
		EnableSideBySide:     false,
		GrokModelOptionID:    "grok-3",
		ImageGenerationCount: 4,
		PromptMetadata: map[string]interface{}{
			"action":       "INPUT",
			"promptSource": "NATURAL",
		},
		RequestFeatures: map[string]bool{
			"eagerTweets":   true,
			"serverHistory": true,
		},
		ReturnCitations:     true,
		ReturnSearchResults: true,
		IsDeepsearch:        false,
		IsReasoning:         false,
		Responses: []GrokRequestMessage{
			{
				Message:         message,
				Sender:          1,
				PromptSource:    "",
				FileAttachments: nil,
			},
		},
	}

	var lastErr error
	for attempt := 0; attempt < maxRetries; attempt++ {
		if attempt > 0 {
			time.Sleep(1 * time.Second)
		}

		bodyBytes, err := json.Marshal(req)
		if err != nil {
			lastErr = fmt.Errorf("failed to marshal payload: %w", err)
			continue
		}

		headers := http.Header{
			"Host":      {"grok.x.com"},
			"x-client-transaction-id": {"jglUm5I3HtYv3B9FPhqeLMP4zpFHqSQJN6W0gpxlyAHTTMDrXC4ZRAGkP4dA8WAb3IByWI3Ig255RlyQLULCsnTvHSbnjQ"},
		}

		respBody, statusCode, err := gc.twitterClient.Post(
			grokAddResponseURL,
			bytes.NewReader(bodyBytes),
			&headers,
		)
		fmt.Println()
		fmt.Println(string(*respBody))
		fmt.Println()
		if err != nil {
			lastErr = fmt.Errorf("request failed: %w", err)
			continue
		}

		if statusCode != http.StatusOK {
			lastErr = fmt.Errorf("unexpected status code: %d", statusCode)
			continue
		}

		// Wait for Grok to process the request
		time.Sleep(3 * time.Second)

		// Get the conversation to see the response
		return nil, nil
	}

	return nil, fmt.Errorf("failed after %d attempts: %w", maxRetries, lastErr)
}

// GetConversation retrieves the current state of a Grok conversation
func (gc *GrokClient) GetConversation(conversationID string) (*GrokResponse, error) {
	variables := map[string]string{
		"restId": conversationID,
	}

	payload := map[string]interface{}{
		"variables": variables,
	}

	var lastErr error
	for attempt := 0; attempt < maxRetries; attempt++ {
		if attempt > 0 {
			time.Sleep(1 * time.Second)
		}

		bodyBytes, err := json.Marshal(payload)
		if err != nil {
			lastErr = fmt.Errorf("failed to marshal payload: %w", err)
			continue
		}

		respBody, statusCode, err := gc.twitterClient.Post(
			gc.twitterClient.BaseURL+grokGetConvEndpoint,
			bytes.NewReader(bodyBytes),
			nil,
		)
		if err != nil {
			lastErr = fmt.Errorf("request failed: %w", err)
			continue
		}

		if statusCode != http.StatusOK {
			lastErr = fmt.Errorf("unexpected status code: %d", statusCode)
			continue
		}

		result := gjson.ParseBytes(*respBody)
		items := result.Get("data.grok_conversation_items_by_rest_id.items").Array()

		var messages []GrokMessage
		for _, item := range items {
			messages = append(messages, GrokMessage{
				SenderType: item.Get("sender_type").String(),
				Message:    item.Get("message").String(),
				Timestamp:  time.UnixMilli(item.Get("created_at").Int()),
			})
		}

		return &GrokResponse{
			ConversationID: conversationID,
			Messages:       messages,
		}, nil
	}

	return nil, fmt.Errorf("failed after %d attempts: %w", maxRetries, lastErr)
}
