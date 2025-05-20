from typing import Any, Dict, Optional
from urllib.parse import urlparse
import logging
import json
import time

import httpx

from x_client_transaction.utils import handle_x_migration
from x_client_transaction import ClientTransaction


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
logging.getLogger("httpx").setLevel(logging.WARNING)



class GrokClient:

    def __init__(
        self,
        auth_token: str,
        ct0: str,
        proxy: Optional[str] = None,
        user_agent: str = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:138.0) Gecko/20100101 Firefox/138.0",
        max_retries: int = 5,
        **kwargs
    ):
        self.user_agent = user_agent
        self.max_retries = max_retries
        
        self.cookies = {
            "auth_token": auth_token,
            "ct0": ct0,
            **kwargs.get("cookies", {})
        }
        
        self.client = httpx.Client(
            http2=True,
            timeout=60,
            cookies=self.cookies,
            proxy=proxy if proxy else None,
            verify=True
        )
        self.client.headers = {
            "authority": "x.com",
            "accept-language": "en-US,en;q=0.9",
            "cache-control": "no-cache",
            "referer": "https://x.com",
            "user-agent": self.user_agent,
            "x-twitter-uctive-user": "yes",
            "x-twitter-client-canguage": "en"
        }
        self.x_home = handle_x_migration(self.client)
        self.ct = ClientTransaction(self.x_home)


    def _headers(self, extra_headers: Optional[Dict[str, str]] = None) -> Dict[str, str]:
        """Generate headers with optional extras"""
        headers = {
            "User-Agent": self.user_agent,
            "Accept": "*/*",
            "Accept-Language": "en-US,en;q=0.5",
            "Accept-Encoding": "gzip, deflate, br, zstd",
            "Referer": "https://x.com/i/grok",
            "x-twitter-auth-type": "OAuth2Session",
            "x-twitter-client-language": "en",
            "x-twitter-active-user": "yes",
            "Origin": "https://x.com",
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-origin",
            "authorization": "Bearer AAAAAAAAAAAAAAAAAAAAANRILgAAAAAAnNwIzUejRCOuH5E6I8xnZz4puTs%3D1Zv7ttfk8LF81IUq16cHjhLTvJu4FA33AGWWjCpTnA",
            "Connection": "keep-alive",
            "Sec-GPC": "1",
            "x-csrf-token": self.cookies["ct0"],
            "TE": "trailers"
        }
        if extra_headers:
            headers.update(extra_headers)
        return headers

    def _request(
        self,
        method: str,
        url: str,
        raise_for_status: bool = True,
        out_put: str = "json",
        **kwargs
    ) -> Optional[Dict[str, Any]]:
        """Base request method with retry logic"""
        for attempt in range(self.max_retries):
            try:
                headers = self._headers(kwargs.pop("extra_headers", None))
                headers["x-client-transaction-id"] = self.ct.generate_transaction_id(method, urlparse(url).path)
                r = self.client.request(
                    method, url,
                    headers=headers,
                    **kwargs
                )
                if raise_for_status:
                    r.raise_for_status()
                if r.text:
                    if out_put.lower() == "json":
                        return r.json()
                    elif out_put.lower() == "text":
                        return r.text
                    else:
                        return r
                return None

            except httpx.HTTPStatusError as e:
                logger.error(f"Attempt [{attempt+1}/{self.max_retries}] failed: {e}")
                if hasattr(e, 'response'):
                    logger.error(f"Status code: {e.response.status_code} Response: {e.response.text}")
                time.sleep(1)
            except Exception as e:
                logger.error(f"Attempt [{attempt+1}/{self.max_retries}] unexpected error: {e}")
                time.sleep(1)
        return None

    def create_conversation(self) -> Optional[Dict[str, Any]]:
        """Create a new Grok conversation"""
        return self._request(
            "POST",
            "https://x.com/i/api/graphql/vvC5uy7pWWHXS2aDi1FZeA/CreateGrokConversation",
            json={
                "variables": {},
                "queryId": "vvC5uy7pWWHXS2aDi1FZeA"
            },
        )

    def send_message(
        self,
        message: str,
        conversation_id: str,
        model: str = "grok-3",
        return_search_results: bool = True,
        return_citations: bool = True
    ) -> Optional[Dict[str, Any]]:
        """Send a message to an existing Grok conversation"""
        return self._request(
            "POST",
            "https://grok.x.com/2/grok/add_response.json",
            content=json.dumps({
                "responses": [{
                    "message": message,
                    "sender": 1,
                    "promptSource": "",
                    "fileAttachments": []
                }],
                "systemPromptName": "",
                "grokModelOptionId": model,
                "conversationId": conversation_id,
                "returnSearchResults": return_search_results,
                "returnCitations": return_citations,
                "promptMetadata": {
                    "promptSource": "NATURAL",
                    "action": "INPUT"
                },
                "imageGenerationCount": 4,
                "requestFeatures": {
                    "eagerTweets": True,
                    "serverHistory": True
                },
                "enableSideBySide": True,
                "toolOverrides": {},
                "isDeepsearch": False,
                "isReasoning": False
            }),
            extra_headers={
                "Content-Type": "text/plain;charset=UTF-8",
            },
            out_put="text"
        )

    def analyze_tweet(
        self,
        tweet_link: str,
        conversation_id: str,
    ) -> Optional[Dict[str, Any]]:
        """Send a message to Grok with detailed payload structure from curl request"""
        return self._request(
            "POST",
            "https://grok.x.com/2/grok/add_response.json",
            content=json.dumps({
                "responses": [{
                    "message": tweet_link,
                    "sender": 1,
                    "promptSource": "post_analysis_details_page"
                }],
                "systemPromptName": "",
                "conversationId": conversation_id,
                "returnSearchResults": True,
                "returnCitations": True,
                "promptMetadata": {
                    "promptSource": "GROK_ANALYZE",
                    "action": "INPUT",
                    "properties": {
                        "element": "BUTTON"
                    }
                },
                "imageGenerationCount": 4,
                "requestFeatures": {
                    "eagerTweets": True,
                    "serverHistory": True
                },
                "enableSideBySide": False,
                "toolOverrides": {}
            }),
            extra_headers={
                "Content-Type": "text/plain;charset=UTF-8",
            },
            out_put="text"
        )

    def get_conversation_history(self, conversation_id: str) -> Optional[Dict[str, Any]]:
        """Get the history of a specific Grok conversation"""
        url = "https://x.com/i/api/graphql/8xmpo8d_Z7qMI4BzMfHG6Q/GrokConversationItemsByRestId"
        features = {
            "creator_subscriptions_tweet_preview_api_enabled": True,
            "premium_content_api_read_enabled": False,
            "communities_web_enable_tweet_community_results_fetch": True,
            "c9s_tweet_anatomy_moderator_badge_enabled": True,
            "responsive_web_grok_analyze_button_fetch_trends_enabled": False,
            "responsive_web_grok_analyze_post_followups_enabled": True,
            "responsive_web_jetfuel_frame": False,
            "responsive_web_grok_share_attachment_enabled": True,
            "articles_preview_enabled": True,
            "responsive_web_edit_tweet_api_enabled": True,
            "graphql_is_translatable_rweb_tweet_is_translatable_enabled": True,
            "view_counts_everywhere_api_enabled": True,
            "longform_notetweets_consumption_enabled": True,
            "responsive_web_twitter_article_tweet_consumption_enabled": True,
            "tweet_awards_web_tipping_enabled": False,
            "responsive_web_grok_show_grok_translated_post": False,
            "responsive_web_grok_analysis_button_from_backend": True,
            "creator_subscriptions_quote_tweet_preview_enabled": False,
            "freedom_of_speech_not_reach_fetch_enabled": True,
            "standardized_nudges_misinfo": True,
            "tweet_with_visibility_results_prefer_gql_limited_actions_policy_enabled": True,
            "longform_notetweets_rich_text_read_enabled": True,
            "longform_notetweets_inline_media_enabled": True,
            "profile_label_improvements_pcf_label_in_post_enabled": True,
            "rweb_tipjar_consumption_enabled": True,
            "verified_phone_label_enabled": False,
            "responsive_web_grok_image_annotation_enabled": True,
            "responsive_web_graphql_skip_user_profile_image_extensions_enabled": False,
            "responsive_web_graphql_timeline_navigation_enabled": True,
            "responsive_web_enhance_cards_enabled": False
        }

        return self._request(
            "GET",
            url,
            params={
                "variables": json.dumps({"restId": conversation_id}),
                "features": json.dumps(features)
            },
            extra_headers={
                "content-type": "application/json",
            }
        )

    def parse_grok_response(self, r: str) -> str:
        """
        Parse Grok's streaming response to extract and combine all message parts.
        
        Args:
            response: The raw response dictionary from send_message
            
        Returns:
            str: The combined clean message from Grok
        """
        if not r:
            return ""
        
        messages = []
        for line in r.splitlines():
            line = line.strip()
            if not line:
                continue
            
            try:
                data = json.loads(line)
                if "result" in data and "message" in data["result"]:
                    messages.append(data["result"]["message"])
            except json.JSONDecodeError:
                continue
        
        return "".join(messages)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.client.close()

