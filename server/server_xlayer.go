package server

import (
	"strings"
)

// getAllowedOrigins returns a map of allowed origins
func getAllowedOrigins(listAllowedOrigins string) map[string]bool {
	ret := make(map[string]bool)
	origins := strings.Split(listAllowedOrigins, ",")
	for _, origin := range origins {
		if origin = strings.TrimSpace(origin); origin != "" {
			ret[strings.ToLower(origin)] = true
		}
	}

	return ret
}
