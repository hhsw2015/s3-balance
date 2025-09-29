package middleware

import (
	"encoding/base64"
	"net/http"
	"strings"
)

// AuthConfig controls Basic Auth validation.
type AuthConfig struct {
	Required  bool
	AccessKey string
	SecretKey string
	OnError   func(http.ResponseWriter, string, string, string)
}

// BasicAuth enforces static access/secret key authentication when Required is true.
func BasicAuth(cfg AuthConfig) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if !cfg.Required {
				next.ServeHTTP(w, r)
				return
			}

			authHeader := r.Header.Get("Authorization")
			if !strings.HasPrefix(authHeader, "Basic ") {
				requireAuth(w, cfg)
				return
			}

			payload := strings.TrimPrefix(authHeader, "Basic ")
			decoded, err := base64.StdEncoding.DecodeString(payload)
			if err != nil {
				requireAuth(w, cfg)
				return
			}

			parts := strings.SplitN(string(decoded), ":", 2)
			if len(parts) != 2 {
				requireAuth(w, cfg)
				return
			}

			if parts[0] != cfg.AccessKey {
				invokeOnError(w, cfg, "InvalidAccessKeyId", "The AWS Access Key Id you provided does not match the configured key.")
				return
			}
			if parts[1] != cfg.SecretKey {
				invokeOnError(w, cfg, "SignatureDoesNotMatch", "The request signature we calculated does not match the signature you provided.")
				return
			}

			next.ServeHTTP(w, r)
		})
	}
}

func requireAuth(w http.ResponseWriter, cfg AuthConfig) {
	w.Header().Set("WWW-Authenticate", "Basic realm=\"s3-balance\"")
	invokeOnError(w, cfg, "AccessDenied", "Access Denied")
}

func invokeOnError(w http.ResponseWriter, cfg AuthConfig, code, message string) {
	if cfg.OnError != nil {
		cfg.OnError(w, code, message, "")
		return
	}
	http.Error(w, message, http.StatusForbidden)
}
