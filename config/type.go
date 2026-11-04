package config

type Config struct {
	App         string      `mapstructure:"app"`
	Persistence Persistence `mapstructure:"persistence"`
	Server      Server      `mapstructure:"server"`
	Memory      Memory      `mapstructure:"memory"`
	Services    Services    `mapstructure:"services"`
	Grpc        Grpc        `mapstructure:"grpc"`
	Stream      Stream      `mapstructure:"stream"`
	X           X           `mapstructure:"x"`
	Pipelines   Pipelines   `mapstructure:"pipelines"`
	Admin       Admin       `mapstructure:"admin"`
}

// Pipelines toggles the legacy subsystems. Everything defaults to off so the
// service starts with only MongoDB + the following scraper.
type Pipelines struct {
	Tweets     bool `mapstructure:"tweets"`     // token tweet indexer + scrape-cycle scheduler
	Grok       bool `mapstructure:"grok"`       // grok-py calls, grok retry / profile analysis schedulers
	Schedulers bool `mapstructure:"schedulers"` // legacy cron jobs (health, following refresh, profile updater)
	Grpc       bool `mapstructure:"grpc"`       // gRPC server + outbound gRPC clients
}

type Admin struct {
	// Token required in the X-Admin-Token header for /admin/* endpoints.
	// Set via ADMIN_TOKEN. Empty disables the admin endpoints.
	Token string `mapstructure:"token"`
}

// X holds everything tied to the x.com web client. Query IDs and feature
// flags rotate with X web deploys, so they live here instead of in code.
type X struct {
	BaseURL   string            `mapstructure:"baseUrl"`
	Bearer    string            `mapstructure:"bearer"` // public web-app bearer, not a secret
	UserAgent string            `mapstructure:"userAgent"`
	Ops       map[string]XOp    `mapstructure:"ops"`
	Scrape    XScrape           `mapstructure:"scrape"`
	Headers   map[string]string `mapstructure:"headers"`
}

// XOp is one GraphQL operation. Features / FieldToggles / ExtraVariables are
// raw JSON strings (paste them straight from the browser's request URL).
// They are strings, not YAML maps, because viper lower-cases map keys and
// X's field toggles are camelCase.
type XOp struct {
	QueryID        string `mapstructure:"queryId"`
	Features       string `mapstructure:"features"`
	FieldToggles   string `mapstructure:"fieldToggles"`
	ExtraVariables string `mapstructure:"extraVariables"`
}

type XScrape struct {
	PageSize           int `mapstructure:"pageSize"`           // users per Following page (X caps around 50-100)
	PageDelayMs        int `mapstructure:"pageDelayMs"`        // pause between pages on one account
	MaxAttemptsPerPage int `mapstructure:"maxAttemptsPerPage"` // across account rotations
	MaxAccountSwaps    int `mapstructure:"maxAccountSwaps"`    // per handle, protects the pool
	RateLimitCooldownS int `mapstructure:"rateLimitCooldownS"` // fallback when x-rate-limit-reset is absent
	BackoffBaseMs      int `mapstructure:"backoffBaseMs"`
	BackoffMaxMs       int `mapstructure:"backoffMaxMs"`
	LeaseTimeoutS      int `mapstructure:"leaseTimeoutS"` // in_use leases older than this are reclaimable
	AccountGroup       int `mapstructure:"accountGroup"`  // dummy.accounts group to lease from (0 = WORKING_GROUP)
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
	Enabled        bool     `mapstructure:"enabled"`
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
