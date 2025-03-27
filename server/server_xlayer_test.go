package server

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestGetAllowedOrigins(t *testing.T) {
	tests := []struct {
		name               string
		listAllowedOrigins string
		expected           map[string]bool
	}{
		{
			name:               "Single origin",
			listAllowedOrigins: "https://example.com",
			expected: map[string]bool{
				"https://example.com": true,
			},
		},
		{
			name:               "Multiple origins",
			listAllowedOrigins: "https://example.com,https://another.com",
			expected: map[string]bool{
				"https://example.com": true,
				"https://another.com": true,
			},
		},
		{
			name:               "Origin with spaces",
			listAllowedOrigins: " https://example.com, https://another.com ",
			expected: map[string]bool{
				"https://example.com": true,
				"https://another.com": true,
			},
		},
		{
			name:               "Origin with uppercase Letter",
			listAllowedOrigins: " https://example.com, https://ANOTHER.com ",
			expected: map[string]bool{
				"https://example.com": true,
				"https://another.com": true,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := getAllowedOrigins(tt.listAllowedOrigins)

			for origin, expected := range tt.expected {
				if result[origin] != expected {
					t.Errorf("For origin %s, expected %v, got %v", origin, expected, result[origin])
				}
			}

			for origin := range result {
				if _, found := tt.expected[origin]; !found {
					t.Errorf("Unexpected origin found: %s", origin)
				}
			}
		})
	}
}

func TestAllowCORS(t *testing.T) {
	allowedOrigins := map[string]bool{
		"https://allowed.com": true,
	}

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	corsHandler := allowCORS(handler, allowedOrigins)

	t.Run("Allowed Origin", func(t *testing.T) {
		req := httptest.NewRequest("GET", "https://example.com", nil)
		req.Header.Set("Origin", "https://allowed.com")
		rr := httptest.NewRecorder()

		corsHandler.ServeHTTP(rr, req)

		if rr.Code != http.StatusOK {
			t.Errorf("Expected status 200, got %d", rr.Code)
		}
		if rr.Header().Get("Access-Control-Allow-Origin") != "https://allowed.com" {
			t.Errorf("Expected Access-Control-Allow-Origin header to be 'https://allowed.com', got %s", rr.Header().Get("Access-Control-Allow-Origin"))
		}
	})

	t.Run("Disallowed Origin", func(t *testing.T) {
		req := httptest.NewRequest("GET", "https://example.com", nil)
		req.Header.Set("Origin", "https://disallowed.com")
		rr := httptest.NewRecorder()

		corsHandler.ServeHTTP(rr, req)

		if rr.Code != http.StatusForbidden {
			t.Errorf("Expected status 403, got %d", rr.Code)
		}
	})
}
