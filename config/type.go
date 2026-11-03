package config

type Config struct {
	App          string       `mapstructure:"app"`
	Persistence  Persistence  `mapstructure:"persistence"`
	Server       Server       `mapstructure:"server"`
	Memory       Memory       `mapstructure:"memory"`
	Services     Services     `mapstructure:"services"`
	Grpc         Grpc         `mapstructure:"grpc"`
	Stream       Stream       `mapstructure:"stream"`
	X            XConfig      `mapstructure:"x"`
	Features     Features     `mapstructure:"features"`
	FollowScrape FollowScrape `mapstructure:"followscrape"`
}

// Features switches the heavy parts of the service on. Everything defaults to
// off, so the service starts with only MongoDB and the HTTP admin API.
type Features struct {
	TweetPipeline bool `mapstructure:"tweetpipeline"` // tweet scraping + schedulers
	Grok          bool `mapstructure:"grok"`          // Grok classification and its schedulers
	Kafka         bool `mapstructure:"kafka"`         // Kafka producers (following events, saas events)
	GrpcClients   bool `mapstructure:"grpcclients"`   // dial the accountable/saas gRPC services
	GrpcServer    bool `mapstructure:"grpcserver"`    // serve the gRPC API
	AccountHealth bool `mapstructure:"accounthealth"` // cron that probes every pooled account
}

// XConfig holds everything about how we talk to x.com. The GraphQL query ids
// and feature flags change whenever the web app is redeployed, so they live
// here and can be overridden without a rebuild.
type XConfig struct {
	BaseURL    string                      `mapstructure:"baseurl"`
	ProxyURL   string                      `mapstructure:"proxyurl"`
	Operations map[string]GraphQLOperation `mapstructure:"operations"`
}

type GraphQLOperation struct {
	QueryID      string          `mapstructure:"queryid"`
	Name         string          `mapstructure:"name"`
	Features     map[string]bool `mapstructure:"features"`
	FieldToggles map[string]bool `mapstructure:"fieldtoggles"`
}

// FollowScrape tunes the on-demand following scraper. Zero values fall back to
// the defaults in followscrape.Options.
type FollowScrape struct {
	PageSize           int `mapstructure:"pagesize"`
	MaxPages           int `mapstructure:"maxpages"`           // 0 = until the end
	MaxAttemptsPerPage int `mapstructure:"maxattemptsperpage"` // tries per page across accounts
	PageDelayMs        int `mapstructure:"pagedelayms"`        // pause between pages
	RateLimitMaxWaitS  int `mapstructure:"ratelimitmaxwaits"`  // longest we sleep on a 429 before swapping account
	CooldownMinutes    int `mapstructure:"cooldownminutes"`    // how long a rate limited account rests
}

type Persistence struct {
	MongoDB MongoDB `mapstructure:"mongodb"`
	S3      S3      `mapstructure:"s3"`
}
type Memory struct {
	Redis Redis `mapstructure:"redis"`
}
type Redis struct {
	Url      string `mapstructure:"url"`
	Password string `mapstructure:"password"`
	User     string `mapstructure:"user"`
	Port     string `mapstructure:"port"`
	Tls      bool   `mapstructure:"tls"`
}
type S3 struct {
	Key            string `mapstructure:"key"`
	Secret         string `mapstructure:"secret"`
	Endpoint       string `mapstructure:"endpoint"`
	CDNEndpoint    string `mapstructure:"cdnEndpoint"`
	ForcePathStyle bool   `mapstructure:"forcePathStyle"`
	Bucket         string `mapstructure:"bucket"`
}
type Server struct {
	Port string `mapstructure:"port"`
}

type MongoDB struct {
	URL      string `mapstructure:"url"`
	Database string `mapstructure:"database"`
}

type Stream struct {
	Kafka Kafka `mapstructure:"kafka"`
}

type Kafka struct {
	Brokers        []string `mapstructure:"brokers"`
	UsePrivate     bool     `mapstructure:"usePrivate"`
	UseSASL        bool     `mapstructure:"useSASL"`
	Username       string   `mapstructure:"userName"`
	Password       string   `mapstructure:"password"`
	TraderTopic    string   `mapstructure:"traderTopic"`
	MemeTopic      string   `mapstructure:"memeTopic"`
	FollowingTopic string   `mapstructure:"followingTopic"`
}
type Services struct {
	CoinGeckoServices  Service            `mapstructure:"coingeckoServices"`
	GrokAPIServices    Service            `mapstructure:"grokapiservices"`
	TwitterApiServices TwitterApiServices `mapstructure:"twitterapiservices"`
	Irys               TwitterApiServices `mapstructure:"irys"`
}
type Service struct {
	Host string `mapstructure:"host"`
}

type TwitterApiServices struct {
	Host        string `mapstructure:"host"`
	BearerToken string `mapstructure:"bearertoken"`
}

type Grpc struct {
	Server  GrpcServer  `mapstructure:"server"`
	Clients GrpcClients `mapstructure:"clients"`
}
type GrpcClients struct {
	AccountableServices GrpcClient `mapstructure:"accountableServices"`
	SaasServices        GrpcClient `mapstructure:"saasServices"`
}
type GrpcServer struct {
	Port string `mapstructure:"port"`
}
type GrpcClient struct {
	Host string `mapstructure:"host"`
}
