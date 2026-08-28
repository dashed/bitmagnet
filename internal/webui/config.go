package webui

type Config struct {
	DefaultFrontend string `validate:"omitempty,oneof=angular react"`
}

func NewDefaultConfig() Config {
	return Config{
		DefaultFrontend: defaultFrontendAngular,
	}
}
