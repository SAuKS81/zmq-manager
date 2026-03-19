package kucoin

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"
)

func negotiatePublicWS(connectID string) (string, time.Duration, error) {
	req, err := http.NewRequest(http.MethodPost, wsTokenURL, nil)
	if err != nil {
		return "", 0, err
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", "zmq_manager/kucoin_native")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", 0, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", 0, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", 0, fmt.Errorf("unexpected status %d body=%s", resp.StatusCode, string(body))
	}

	var tokenResp tokenResponse
	if err := json.Unmarshal(body, &tokenResp); err != nil {
		return "", 0, fmt.Errorf("decode token response: %w", err)
	}
	if tokenResp.Code != "200000" {
		return "", 0, fmt.Errorf("token response code=%s body=%s", tokenResp.Code, string(body))
	}
	if tokenResp.Data.Token == "" || len(tokenResp.Data.InstanceServers) == 0 {
		return "", 0, fmt.Errorf("missing token or instanceServers in response")
	}

	server := tokenResp.Data.InstanceServers[0]
	baseURL, err := url.Parse(server.Endpoint)
	if err != nil {
		return "", 0, fmt.Errorf("parse endpoint: %w", err)
	}
	query := baseURL.Query()
	query.Set("token", tokenResp.Data.Token)
	query.Set("connectId", connectID)
	baseURL.RawQuery = query.Encode()

	return baseURL.String(), time.Duration(server.PingInterval) * time.Millisecond, nil
}
