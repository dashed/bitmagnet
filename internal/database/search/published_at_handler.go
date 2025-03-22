package search

import (
	"context"
	"net/http"
	"strings"

	"github.com/bitmagnet-io/bitmagnet/internal/database/query"
	"github.com/gin-gonic/gin"
)

// PublishedAtMiddleware extracts publishedAt parameter from GraphQL queries
func PublishedAtMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		// Only intercept POST requests to GraphQL endpoint
		if c.Request.Method == http.MethodPost && strings.HasSuffix(c.Request.URL.Path, "/graphql") {
			// Process the request body, which contains the GraphQL query
			var requestBody struct {
				Query     string                 `json:"query"`
				Variables map[string]interface{} `json:"variables"`
			}
			
			if err := c.ShouldBindJSON(&requestBody); err == nil {
				// Look for publishedAt in variables
				if requestBody.Variables != nil {
					if facets, ok := requestBody.Variables["input"].(map[string]interface{}); ok {
						if facets, ok := facets["facets"].(map[string]interface{}); ok {
							if publishedAt, ok := facets["publishedAt"].(string); ok && publishedAt != "" {
								// Store publishedAt in the request context
								ctx := context.WithValue(c.Request.Context(), "publishedAt", publishedAt)
								c.Request = c.Request.WithContext(ctx)
							}
						}
					}
				}
			}
			
			// Reset the request body for next middleware
			c.Request.Body = http.NoBody
		}
		
		c.Next()
	}
}

// PublishedAtFromContext retrieves publishedAt parameter from context
func PublishedAtFromContext(ctx context.Context) (string, bool) {
	publishedAt, ok := ctx.Value("publishedAt").(string)
	return publishedAt, ok && publishedAt != ""
}

// GetPublishedAtCriteria creates a criteria from context
func GetPublishedAtCriteria() query.Criteria {
	return query.GenCriteria(func(ctx query.DbContext) (query.Criteria, error) {
		return TorrentContentPublishedAtCriteria(""), nil
	})
}