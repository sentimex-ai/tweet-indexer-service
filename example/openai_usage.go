package main

import (
	"fmt"
	"log"
	"os"

	"github.com/jeetdotso/tweet-indexer-service/scrape/internals/api/openai"
)

func main() {
	apiKey := os.Getenv("OPENAI_API_KEY")
	if apiKey == "" {
		log.Fatal("Please set OPENAI_API_KEY environment variable")
	}

	client := openai.New(apiKey)

	r, err := client.ChatCompletion(
		"gpt-4",
		"Is this tweet explicitly promoting a specific cryptocurrency token(s)? If yes, I want to know what token(s) the author is promoting in the tweet. Do not look at data from the comments section to answer my question. \"buy $NXCP\". output in raw txt strictly follow the following schema {\"isPromoting\": bool,\"tokensPromoted\": list or null}",
	)
	if err != nil {
		log.Fatalf("Error: %v", err)
	}
	fmt.Println("response:")
	fmt.Println(r)

	imageURL := "https://images.pexels.com/photos/20804701/pexels-photo-20804701/free-photo-of-close-up-of-a-mallard-walking-on-a-field.jpeg?auto=compress&cs=tinysrgb&w=1260&h=750&dpr=2"
	prompt := "What's in this image? Describe in detail."
	
	analysis, err := client.AnalyzeImageURL(imageURL, prompt)
	if err != nil {
		log.Fatalf("Image analysis error: %v", err)
	}
	fmt.Println("response:")
	fmt.Println(analysis)
}
