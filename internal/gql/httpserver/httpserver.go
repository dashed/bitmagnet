package httpserver

import (
	"net/http"
	"time"

	"github.com/99designs/gqlgen/graphql"
	"github.com/99designs/gqlgen/graphql/handler"
	"github.com/99designs/gqlgen/graphql/handler/extension"
	"github.com/99designs/gqlgen/graphql/handler/lru"
	"github.com/99designs/gqlgen/graphql/handler/transport"
	"github.com/99designs/gqlgen/graphql/playground"
	"github.com/bitmagnet-io/bitmagnet/internal/httpserver"
	"github.com/bitmagnet-io/bitmagnet/internal/lazy"
	"github.com/bitmagnet-io/bitmagnet/internal/search/graphqlshadow"
	"github.com/gin-gonic/gin"
	"github.com/vektah/gqlparser/v2/ast"
	"go.uber.org/fx"
	"go.uber.org/zap"
)

type Params struct {
	fx.In
	Schema lazy.Lazy[graphql.ExecutableSchema]
	Logger *zap.SugaredLogger
	Shadow *graphqlshadow.Hook
}

type Result struct {
	fx.Out
	Option httpserver.Option `group:"http_server_options"`
}

func New(p Params) Result {
	return Result{
		Option: &builder{
			schema: p.Schema,
			shadow: p.Shadow,
		},
	}
}

type builder struct {
	schema lazy.Lazy[graphql.ExecutableSchema]
	shadow *graphqlshadow.Hook
}

func (builder) Key() string {
	return "graphql"
}

func (b builder) Apply(e *gin.Engine) error {
	schema, err := b.schema.Get()
	if err != nil {
		return err
	}

	gql := newHandler(schema, b.shadow)

	e.POST("/graphql", func(c *gin.Context) {
		gql.ServeHTTP(c.Writer, c.Request)
	})

	pg := playground.Handler("GraphQL playground", "/graphql")

	e.GET("/graphql", func(c *gin.Context) {
		pg.ServeHTTP(c.Writer, c.Request)
	})

	return nil
}

func newServer(es graphql.ExecutableSchema, shadow *graphqlshadow.Hook) *handler.Server {
	srv := handler.New(es)

	srv.AddTransport(transport.Websocket{
		KeepAlivePingInterval: 10 * time.Second,
	})
	srv.AddTransport(transport.Options{})
	srv.AddTransport(transport.GET{})
	srv.AddTransport(transport.POST{})
	srv.AddTransport(transport.MultipartForm{})

	srv.SetQueryCache(lru.New[*ast.QueryDocument](1000))

	srv.Use(extension.Introspection{})
	srv.Use(extension.AutomaticPersistedQuery{
		Cache: lru.New[string](100),
	})

	if shadow != nil {
		srv.AroundResponses(shadow.CaptureResponse)
	}

	return srv
}

// newHandler records request entry before gqlgen runs. The response middleware
// seals the pre-write response-generation duration as soon as its handler
// returns; Finish waits until ServeHTTP completes only to dispatch shadow work.
func newHandler(es graphql.ExecutableSchema, shadow *graphqlshadow.Hook) http.Handler {
	server := newServer(es, shadow)
	if shadow == nil {
		return server
	}

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx, capture := shadow.Begin(r.Context())
		server.ServeHTTP(w, r.WithContext(ctx))
		shadow.Finish(ctx, capture)
	})
}
