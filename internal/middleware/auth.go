package middleware

import (
	"context"
	"fmt"
	"net/http"

	"github.com/DullJZ/s3-validate/pkg/s3validate"
)

// S3SignatureConfig controls S3 signature validation.
type S3SignatureConfig struct {
	Required      func() bool
	Credentials   func() (string, string)
	OnError       func(http.ResponseWriter, string, string, string)
	SignatureHost func() string // 用于签名验证的Host（为空则使用请求的Host）
}

// credentialsProvider implements s3validate.CredentialsProvider interface.
type credentialsProvider struct {
	getCredentials func() (string, string)
}

func (p *credentialsProvider) SecretKey(ctx context.Context, accessKey string) (string, error) {
	expectedAccessKey, secretKey := p.getCredentials()
	if accessKey != expectedAccessKey {
		return "", fmt.Errorf("invalid access key")
	}
	return secretKey, nil
}

// S3Signature enforces AWS Signature V4 authentication when required.
func S3Signature(cfg S3SignatureConfig) func(http.Handler) http.Handler {
	verifier := &s3validate.Verifier{
		Credentials: &credentialsProvider{
			getCredentials: cfg.Credentials,
		},
	}

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			required := false
			if cfg.Required != nil {
				required = cfg.Required()
			}
			if !required {
				next.ServeHTTP(w, r)
				return
			}

			// 如果配置了签名验证的Host，覆盖请求的Host
			if cfg.SignatureHost != nil {
				if signatureHost := cfg.SignatureHost(); signatureHost != "" {
					r.Host = signatureHost
				}
			}

			result, err := verifier.Verify(r.Context(), r)
			if err != nil {
				invokeOnError(w, cfg, "SignatureDoesNotMatch", err.Error())
				return
			}

			// Store verification result in context for potential use by handlers
			_ = result

			next.ServeHTTP(w, r)
		})
	}
}

func invokeOnError(w http.ResponseWriter, cfg S3SignatureConfig, code, message string) {
	if cfg.OnError != nil {
		cfg.OnError(w, code, message, "")
		return
	}
	http.Error(w, message, http.StatusForbidden)
}
