package services

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"
)

// GatewayClient is an authenticated HTTP client to a gateway service.
// It caches the app token and refreshes it when it nears expiry (60s buffer).
type GatewayClient struct {
	baseURL         string
	authURL         string
	appInstanceCode string
	apiKey          string
	hc              *http.Client

	mu     sync.Mutex
	tok    string
	expiry time.Time
}

// Do issues an authenticated request to baseURL+path.
// Acquires/refreshes the app token as needed, then sets Authorization: Bearer.
func (c *GatewayClient) Do(ctx context.Context, method, path string, body io.Reader) (*http.Response, error) {
	tok, err := c.getToken(ctx)
	if err != nil {
		return nil, fmt.Errorf("gateway: get token: %w", err)
	}
	url := strings.TrimRight(c.baseURL, "/") + "/" + strings.TrimLeft(path, "/")
	req, err := http.NewRequestWithContext(ctx, method, url, body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+tok)
	return c.hc.Do(req)
}

// getToken returns a valid token, re-authenticating if needed.
//
// The mutex is held across the authenticate HTTP call to coalesce concurrent
// refreshes: the first caller does the api_login; the rest re-check after
// acquiring the lock and return the cached token. On auth failure, concurrent
// callers stagger by ~10s (the auth timeout) and each retries, but the consume
// layer's exponential back-off limits the overall burst.
func (c *GatewayClient) getToken(ctx context.Context) (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Double-checked: check expiry inside the lock.
	if c.tok != "" && time.Now().Add(60*time.Second).Before(c.expiry) {
		return c.tok, nil
	}
	if err := c.authenticate(ctx); err != nil {
		return "", err
	}
	return c.tok, nil
}

// authenticate POSTs credentials to the api_login endpoint and caches the token.
func (c *GatewayClient) authenticate(ctx context.Context) error {
	body, err := json.Marshal(map[string]string{
		"app_instance_code": c.appInstanceCode,
		"api_key":           c.apiKey,
	})
	if err != nil {
		return fmt.Errorf("marshal login body: %w", err)
	}
	authCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(authCtx, http.MethodPost, c.authURL, strings.NewReader(string(body)))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.hc.Do(req)
	if err != nil {
		return fmt.Errorf("gateway login: %w", err)
	}
	defer func() {
		_, _ = io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("gateway login returned %d", resp.StatusCode)
	}

	var result struct {
		Access string `json:"access"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return fmt.Errorf("decode login response: %w", err)
	}
	if result.Access == "" {
		return fmt.Errorf("gateway login: empty access token")
	}

	expiry, err := jwtExpiry(result.Access)
	if err != nil {
		return fmt.Errorf("gateway login: parse token expiry: %w", err)
	}
	c.tok = result.Access
	c.expiry = expiry
	return nil
}

// jwtExpiry decodes the exp claim from a JWT without verifying the signature.
func jwtExpiry(token string) (time.Time, error) {
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return time.Time{}, fmt.Errorf("invalid JWT format")
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return time.Time{}, fmt.Errorf("decode JWT payload: %w", err)
	}
	var claims struct {
		Exp int64 `json:"exp"`
	}
	if err := json.Unmarshal(payload, &claims); err != nil {
		return time.Time{}, fmt.Errorf("parse JWT claims: %w", err)
	}
	if claims.Exp == 0 {
		return time.Time{}, fmt.Errorf("JWT missing exp claim")
	}
	return time.Unix(claims.Exp, 0), nil
}
