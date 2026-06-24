package main

import (
	"bytes"
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"
)

// githubAPIBase is the GitHub REST API root. It's a var so tests can point the
// token exchange at an httptest server.
var githubAPIBase = "https://api.github.com"

// runGitHubToken mints a short-lived GitHub App installation access token from
// an App ID + installation ID + RSA private key and prints ONLY the token to
// stdout, so a caller can capture it with `TOKEN=$(hclexp github-token ...)`.
// Diagnostics go to stderr; any failure exits non-zero. See issue #61.
func runGitHubToken(args []string) {
	fs := flag.NewFlagSet("hclexp github-token", flag.ExitOnError)
	appID := fs.String("app-id", "", "GitHub App ID (required)")
	installationID := fs.String("installation-id", "", "GitHub App installation ID (required)")
	keyFile := fs.String("private-key-file", "", "path to the App private key PEM; defaults to the GITHUB_APP_PRIVATE_KEY env var")
	repo := fs.String("repo", "", "optional owner/name: scope the token to this single repository and report the resolved permissions on stderr")
	_ = fs.Parse(args)

	if *appID == "" || *installationID == "" {
		fmt.Fprintln(os.Stderr, "github-token: -app-id and -installation-id are required")
		os.Exit(2)
	}

	pemBytes, err := loadAppPrivateKey(*keyFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "github-token: %v\n", err)
		os.Exit(1)
	}
	key, err := parseRSAPrivateKey(pemBytes)
	if err != nil {
		fmt.Fprintf(os.Stderr, "github-token: %v\n", err)
		os.Exit(1)
	}

	jwt, err := buildAppJWT(*appID, key, time.Now())
	if err != nil {
		fmt.Fprintf(os.Stderr, "github-token: %v\n", err)
		os.Exit(1)
	}

	var repos []string
	if *repo != "" {
		// The access_tokens API scopes by repository name (within the
		// installation's owner), so take the part after any "owner/".
		name := *repo
		if i := strings.LastIndex(name, "/"); i >= 0 {
			name = name[i+1:]
		}
		repos = []string{name}
	}

	resp, err := requestInstallationToken(context.Background(), githubAPIBase, *installationID, jwt, repos)
	if err != nil {
		fmt.Fprintf(os.Stderr, "github-token: %v\n", err)
		os.Exit(1)
	}

	if *repo != "" {
		fmt.Fprintf(os.Stderr, "github-token: minted for %s — repository_selection=%s permissions=%v expires_at=%s\n",
			*repo, resp.RepositorySelection, resp.Permissions, resp.ExpiresAt)
	}

	// Only the token on stdout, nothing else.
	fmt.Println(resp.Token)
}

// loadAppPrivateKey returns the PEM private key bytes from the given file, or
// from GITHUB_APP_PRIVATE_KEY when no file is given (so the key need not touch
// disk).
func loadAppPrivateKey(file string) ([]byte, error) {
	if file != "" {
		b, err := os.ReadFile(file)
		if err != nil {
			return nil, fmt.Errorf("reading private key file: %w", err)
		}
		return b, nil
	}
	if v := os.Getenv("GITHUB_APP_PRIVATE_KEY"); v != "" {
		return []byte(v), nil
	}
	return nil, errors.New("no private key: set GITHUB_APP_PRIVATE_KEY or pass -private-key-file")
}

// parseRSAPrivateKey decodes a PEM-encoded RSA private key in either PKCS#1
// (`RSA PRIVATE KEY`, the form GitHub issues) or PKCS#8 (`PRIVATE KEY`) format.
func parseRSAPrivateKey(pemBytes []byte) (*rsa.PrivateKey, error) {
	block, _ := pem.Decode(pemBytes)
	if block == nil {
		return nil, errors.New("invalid PEM: no private key block found")
	}
	if k, err := x509.ParsePKCS1PrivateKey(block.Bytes); err == nil {
		return k, nil
	}
	k8, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("parsing private key: %w", err)
	}
	rk, ok := k8.(*rsa.PrivateKey)
	if !ok {
		return nil, errors.New("private key is not RSA")
	}
	return rk, nil
}

// buildAppJWT builds the RS256 JWT GitHub expects for App authentication: iss =
// App ID, iat backdated 60s for clock skew, exp 9 minutes out (under GitHub's
// 10-minute cap).
func buildAppJWT(appID string, key *rsa.PrivateKey, now time.Time) (string, error) {
	header := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"RS256","typ":"JWT"}`))

	claims := map[string]any{
		"iat": now.Add(-60 * time.Second).Unix(),
		"exp": now.Add(9 * time.Minute).Unix(),
		"iss": appID,
	}
	claimsJSON, err := json.Marshal(claims)
	if err != nil {
		return "", fmt.Errorf("encoding JWT claims: %w", err)
	}
	payload := base64.RawURLEncoding.EncodeToString(claimsJSON)

	signingInput := header + "." + payload
	digest := sha256.Sum256([]byte(signingInput))
	sig, err := rsa.SignPKCS1v15(rand.Reader, key, crypto.SHA256, digest[:])
	if err != nil {
		return "", fmt.Errorf("signing JWT: %w", err)
	}
	return signingInput + "." + base64.RawURLEncoding.EncodeToString(sig), nil
}

// installationToken is the subset of GitHub's access_tokens response we use.
type installationToken struct {
	Token               string         `json:"token"`
	ExpiresAt           string         `json:"expires_at"`
	Permissions         map[string]any `json:"permissions"`
	RepositorySelection string         `json:"repository_selection"`
}

// requestInstallationToken exchanges an App JWT for an installation access token
// via POST /app/installations/{id}/access_tokens. When repos is non-empty the
// token is scoped to those repositories. The default HTTP transport honours
// HTTPS_PROXY/HTTP_PROXY.
func requestInstallationToken(ctx context.Context, apiBase, installationID, jwt string, repos []string) (*installationToken, error) {
	url := fmt.Sprintf("%s/app/installations/%s/access_tokens", apiBase, installationID)

	var body io.Reader
	if len(repos) > 0 {
		b, err := json.Marshal(map[string]any{"repositories": repos})
		if err != nil {
			return nil, fmt.Errorf("encoding request body: %w", err)
		}
		body = bytes.NewReader(b)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, body)
	if err != nil {
		return nil, fmt.Errorf("building request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+jwt)
	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("X-GitHub-Api-Version", "2022-11-28")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("requesting installation token: %w", err)
	}
	defer resp.Body.Close()

	data, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode != http.StatusCreated {
		return nil, fmt.Errorf("github returned %s: %s", resp.Status, strings.TrimSpace(string(data)))
	}

	var tok installationToken
	if err := json.Unmarshal(data, &tok); err != nil {
		return nil, fmt.Errorf("decoding token response: %w", err)
	}
	if tok.Token == "" {
		return nil, errors.New("github response contained no token")
	}
	return &tok, nil
}
