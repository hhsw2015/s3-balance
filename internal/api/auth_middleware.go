package api

import (
	"encoding/base64"
	"net/http"
	"strings"
)

// authMiddleware 处理 Basic Auth 校验
func (h *S3Handler) authMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !h.authRequired {
			next.ServeHTTP(w, r)
			return
		}

		authHeader := r.Header.Get("Authorization")
		if authHeader == "" {
			h.requireAuth(w)
			return
		}

		if strings.HasPrefix(authHeader, "Basic ") {
			payload := strings.TrimPrefix(authHeader, "Basic ")
			decoded, err := base64.StdEncoding.DecodeString(payload)
			if err != nil {
				h.requireAuth(w)
				return
			}

			parts := strings.SplitN(string(decoded), ":", 2)
			if len(parts) != 2 {
				h.requireAuth(w)
				return
			}

			if parts[0] != h.accessKey {
				h.sendS3Error(w, "InvalidAccessKeyId", "The AWS Access Key Id you provided does not match the configured key.", "")
				return
			}
			if parts[1] != h.secretKey {
				h.sendS3Error(w, "SignatureDoesNotMatch", "The request signature we calculated does not match the signature you provided.", "")
				return
			}

			next.ServeHTTP(w, r)
			return
		}

		h.requireAuth(w)
	})
}

func (h *S3Handler) requireAuth(w http.ResponseWriter) {
	w.Header().Set("WWW-Authenticate", "Basic realm=\"s3-balance\"")
	h.sendS3Error(w, "AccessDenied", "Access Denied", "")
}
