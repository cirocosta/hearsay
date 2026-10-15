package codex

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"os"
	"strings"
	"time"
)

type credentials struct{ Token, Account string }

// readCredentials follows gx's read-only external Codex credential path.
// gx-owned flat credential files are accepted too, without token rotation.
func readCredentials(path string) (credentials, error) {
	file, err := os.Open(path)
	if err != nil {
		return credentials{}, err
	}
	defer func() { _ = file.Close() }()
	data, err := io.ReadAll(io.LimitReader(file, (64<<10)+1))
	if err != nil {
		return credentials{}, err
	}
	if len(data) > 64<<10 {
		return credentials{}, errors.New("credential file exceeds 64 KiB")
	}
	return decodeCredentials(data, time.Now())
}

func decodeCredentials(data []byte, now time.Time) (credentials, error) {
	var file struct {
		LoggedOut bool   `json:"logged_out"`
		Token     string `json:"access_token"`
		Account   string `json:"account_id"`
		Tokens    struct {
			Token   string `json:"access_token"`
			Account string `json:"account_id"`
		} `json:"tokens"`
	}
	if err := json.Unmarshal(data, &file); err != nil {
		return credentials{}, errors.New("invalid Codex credential JSON")
	}
	if file.LoggedOut {
		return credentials{}, errors.New("codex credentials are logged out")
	}
	if file.Tokens.Token != "" {
		file.Token, file.Account = file.Tokens.Token, file.Tokens.Account
	}
	expires, account, err := credentialClaims(file.Token)
	if err != nil {
		return credentials{}, err
	}
	if !now.Add(time.Minute).Before(expires) {
		return credentials{}, errors.New("codex login expires soon; refresh it with codex login")
	}
	if file.Account == "" {
		file.Account = account
	}
	if file.Account == "" {
		return credentials{}, errors.New("codex login has no account ID")
	}
	return credentials{Token: file.Token, Account: file.Account}, nil
}

func credentialClaims(token string) (time.Time, string, error) {
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return time.Time{}, "", errors.New("invalid Codex token format")
	}
	data, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return time.Time{}, "", errors.New("invalid Codex token claims")
	}
	var claims struct {
		Expires int64 `json:"exp"`
		Auth    struct {
			Account string `json:"chatgpt_account_id"`
		} `json:"https://api.openai.com/auth"`
	}
	if json.Unmarshal(data, &claims) != nil || claims.Expires <= 0 {
		return time.Time{}, "", errors.New("codex token has no valid expiry")
	}
	return time.Unix(claims.Expires, 0), claims.Auth.Account, nil
}
