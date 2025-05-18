package gemini

import (
	"context"
	"fmt"

	"google.golang.org/genai"
)

type Client struct {
	client *genai.Client
}

func New(apiKey string) (*Client, error) {
	ctx := context.Background()
	
	client, err := genai.NewClient(ctx, &genai.ClientConfig{
		APIKey:   apiKey,
		Backend:  genai.BackendGeminiAPI,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create client: %w", err)
	}

	return &Client{client: client}, nil
}

func (c *Client) GenerateText(prompt string) (string, error) {
	ctx := context.Background()
	
	resp, err := c.client.Models.GenerateContent(ctx, "gemini-2.0-flash", []*genai.Content{
		{Parts: []*genai.Part{
			{Text: prompt},
		}},
	}, nil)
	if err != nil {
		return "", fmt.Errorf("failed to generate content: %w", err)
	}

	if len(resp.Candidates) == 0 || len(resp.Candidates[0].Content.Parts) == 0 {
		return "", fmt.Errorf("empty response from Gemini")
	}

	part := resp.Candidates[0].Content.Parts[0]
	if part.Text == "" {
		return "", fmt.Errorf("unexpected response type from Gemini")
	}

	return part.Text, nil
}

func (c *Client) GenerateMultimodal(textPrompt string, imageData []byte, mimeType string) (string, error) {
	ctx := context.Background()
	
	resp, err := c.client.Models.GenerateContent(ctx, "gemini-pro-vision", []*genai.Content{
		{Parts: []*genai.Part{
			{Text: textPrompt},
			{InlineData: &genai.Blob{
				Data:     imageData,
				MIMEType: mimeType,
			}},
		}},
	}, nil)
	if err != nil {
		return "", fmt.Errorf("failed to generate content: %w", err)
	}

	if len(resp.Candidates) == 0 || len(resp.Candidates[0].Content.Parts) == 0 {
		return "", fmt.Errorf("empty response from Gemini")
	}

	part := resp.Candidates[0].Content.Parts[0]
	if part.Text == "" {
		return "", fmt.Errorf("unexpected response type from Gemini")
	}

	return part.Text, nil
}
