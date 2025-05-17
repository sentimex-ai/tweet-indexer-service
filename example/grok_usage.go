package main

import (
	"fmt"
	"log"
	
	twitterAPI "github.com/jeetdotso/tweet-indexer-service/scrape/internals/api"
)

func main() {
	// Initialize client with your auth tokens
	client := twitterAPI.NewGrokClient("auth_token", "ct0")
	
	// Optionally change the base URL if not running locally
	// client.SetBaseURL("http://your-api-url:8000")
	
	// Create a new conversation
	convID, err := client.CreateConversation("")
	if err != nil {
		log.Fatalf("Failed to create conversation: %v", err)
	}
	fmt.Printf("Created conversation: %s\n", convID)
	
	// Send a message
	response, _, err := client.SendMessage("Hello Grok!", convID, "grok-3", "", false)
	if err != nil {
		log.Fatalf("Failed to send message: %v", err)
	}
	fmt.Printf("Grok response: %s\n", response)
	
	// Get conversation history
	history, err := client.GetConversationHistory(convID, "")
	if err != nil {
		log.Fatalf("Failed to get history: %v", err)
	}
	fmt.Printf("Conversation history: %s\n", history)
	
	// Generate a transaction ID
	txID, err := client.GenerateTransactionID("POST", "/2/grok/add_response.json", "")
	if err != nil {
		log.Fatalf("Failed to generate transaction ID: %v", err)
	}
	fmt.Printf("Transaction ID: %s\n", txID)
}
