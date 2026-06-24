package main

import (
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func testRSAKey(t *testing.T) *rsa.PrivateKey {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	return key
}

func TestParseRSAPrivateKey(t *testing.T) {
	key := testRSAKey(t)

	t.Run("pkcs1 (RSA PRIVATE KEY)", func(t *testing.T) {
		p := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})
		got, err := parseRSAPrivateKey(p)
		require.NoError(t, err)
		assert.Equal(t, key.N, got.N)
	})

	t.Run("pkcs8 (PRIVATE KEY)", func(t *testing.T) {
		der, err := x509.MarshalPKCS8PrivateKey(key)
		require.NoError(t, err)
		p := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der})
		got, err := parseRSAPrivateKey(p)
		require.NoError(t, err)
		assert.Equal(t, key.N, got.N)
	})

	t.Run("invalid PEM", func(t *testing.T) {
		_, err := parseRSAPrivateKey([]byte("not a pem"))
		require.Error(t, err)
	})
}

func TestBuildAppJWT(t *testing.T) {
	key := testRSAKey(t)
	now := time.Unix(1_700_000_000, 0)

	tok, err := buildAppJWT("123456", key, now)
	require.NoError(t, err)

	parts := strings.Split(tok, ".")
	require.Len(t, parts, 3)

	hdr, err := base64.RawURLEncoding.DecodeString(parts[0])
	require.NoError(t, err)
	assert.JSONEq(t, `{"alg":"RS256","typ":"JWT"}`, string(hdr))

	claimsRaw, err := base64.RawURLEncoding.DecodeString(parts[1])
	require.NoError(t, err)
	var claims map[string]any
	require.NoError(t, json.Unmarshal(claimsRaw, &claims))
	assert.Equal(t, "123456", claims["iss"])
	assert.Equal(t, float64(now.Add(-60*time.Second).Unix()), claims["iat"])
	assert.Equal(t, float64(now.Add(9*time.Minute).Unix()), claims["exp"])
	// exp must stay under GitHub's 10-minute ceiling.
	assert.LessOrEqual(t, int64(claims["exp"].(float64))-int64(claims["iat"].(float64)), int64(600))

	// The signature must verify against the public key.
	sig, err := base64.RawURLEncoding.DecodeString(parts[2])
	require.NoError(t, err)
	digest := sha256.Sum256([]byte(parts[0] + "." + parts[1]))
	require.NoError(t, rsa.VerifyPKCS1v15(&key.PublicKey, crypto.SHA256, digest[:], sig))
}

func TestRequestInstallationToken(t *testing.T) {
	t.Run("success prints token; sends correct request", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			assert.Equal(t, http.MethodPost, r.Method)
			assert.Equal(t, "/app/installations/999/access_tokens", r.URL.Path)
			assert.Equal(t, "Bearer my.jwt.here", r.Header.Get("Authorization"))
			assert.Equal(t, "application/vnd.github+json", r.Header.Get("Accept"))
			assert.Equal(t, "2022-11-28", r.Header.Get("X-GitHub-Api-Version"))
			w.WriteHeader(http.StatusCreated)
			_, _ = io.WriteString(w, `{"token":"ghs_abc","expires_at":"2026-01-01T00:00:00Z","permissions":{"contents":"write"},"repository_selection":"selected"}`)
		}))
		defer srv.Close()

		tok, err := requestInstallationToken(context.Background(), srv.URL, "999", "my.jwt.here", nil)
		require.NoError(t, err)
		assert.Equal(t, "ghs_abc", tok.Token)
		assert.Equal(t, "selected", tok.RepositorySelection)
		assert.Equal(t, "write", tok.Permissions["contents"])
	})

	t.Run("repo scoping puts repositories in the body", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			body, _ := io.ReadAll(r.Body)
			assert.JSONEq(t, `{"repositories":["clickhouse-schema"]}`, string(body))
			w.WriteHeader(http.StatusCreated)
			_, _ = io.WriteString(w, `{"token":"ghs_scoped"}`)
		}))
		defer srv.Close()

		tok, err := requestInstallationToken(context.Background(), srv.URL, "1", "j", []string{"clickhouse-schema"})
		require.NoError(t, err)
		assert.Equal(t, "ghs_scoped", tok.Token)
	})

	t.Run("non-201 is an error including the status", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = io.WriteString(w, `{"message":"Bad credentials"}`)
		}))
		defer srv.Close()

		_, err := requestInstallationToken(context.Background(), srv.URL, "1", "j", nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "401")
		assert.Contains(t, err.Error(), "Bad credentials")
	})

	t.Run("201 without a token is an error", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusCreated)
			_, _ = io.WriteString(w, `{}`)
		}))
		defer srv.Close()

		_, err := requestInstallationToken(context.Background(), srv.URL, "1", "j", nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "no token")
	})
}

func TestLoadAppPrivateKey(t *testing.T) {
	t.Run("from file", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "key.pem")
		require.NoError(t, os.WriteFile(path, []byte("PEMDATA"), 0o600))
		got, err := loadAppPrivateKey(path)
		require.NoError(t, err)
		assert.Equal(t, "PEMDATA", string(got))
	})

	t.Run("from env when no file", func(t *testing.T) {
		t.Setenv("GITHUB_APP_PRIVATE_KEY", "ENVPEM")
		got, err := loadAppPrivateKey("")
		require.NoError(t, err)
		assert.Equal(t, "ENVPEM", string(got))
	})

	t.Run("missing both is an error", func(t *testing.T) {
		t.Setenv("GITHUB_APP_PRIVATE_KEY", "")
		_, err := loadAppPrivateKey("")
		require.Error(t, err)
	})
}

// TestGitHubToken_EndToEnd exercises the full JWT → exchange path with a real
// RSA key and a stub GitHub API, verifying the JWT the command sends is valid.
func TestGitHubToken_EndToEnd(t *testing.T) {
	key := testRSAKey(t)
	jwt, err := buildAppJWT("42", key, time.Now())
	require.NoError(t, err)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Verify the JWT the client sent is well-formed and correctly signed.
		bearer := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		parts := strings.Split(bearer, ".")
		require.Len(t, parts, 3)
		sig, _ := base64.RawURLEncoding.DecodeString(parts[2])
		digest := sha256.Sum256([]byte(parts[0] + "." + parts[1]))
		assert.NoError(t, rsa.VerifyPKCS1v15(&key.PublicKey, crypto.SHA256, digest[:], sig))
		w.WriteHeader(http.StatusCreated)
		_, _ = io.WriteString(w, `{"token":"ghs_final"}`)
	}))
	defer srv.Close()

	tok, err := requestInstallationToken(context.Background(), srv.URL, "7", jwt, nil)
	require.NoError(t, err)
	assert.Equal(t, "ghs_final", tok.Token)
}
