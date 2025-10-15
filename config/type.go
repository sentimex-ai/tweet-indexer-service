package config

type Config struct {
	App         string      `mapstructure:"app"`
	Persistence Persistence `mapstructure:"persistence"`
	Server      Server      `mapstructure:"server"`
	Memory      Memory      `mapstructure:"memory"`
	Services    Services    `mapstructure:"services"`
	Grpc        Grpc        `mapstructure:"grpc"`
	Stream      Stream      `mapstructure:"stream"`
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
