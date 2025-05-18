package main

import (
	"fmt"
	"log"
	"os"

	"github.com/jeetdotso/tweet-indexer-service/scrape/internals/api/gemini"
)

func main() {
	apiKey := os.Getenv("GEMINI_API_KEY")
	if apiKey == "" {
		log.Fatal("Please set GEMINI_API_KEY environment variable")
	}

	client, err := gemini.New(apiKey)
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}

	r, err := client.GenerateText("Is this tweet explicitly promoting a specific cryptocurrency token(s)? If yes, I want to know what token(s) the author is promoting in the tweet. Do not look at data from the comments section to answer my question. \"buy $NXCP\". output in raw txt strictly follow the following schema {\"isPromoting\": bool,\"tokensPromoted\": list or null}")
	if err != nil {
		log.Fatalf("Error: %v", err)
	}

	fmt.Println("Gemini response:")
	fmt.Println(r)
}

