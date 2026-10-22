package fetch

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha1"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"net/url"
	"sort"
	"strings"
)

type ipOptions struct{ ConsumerKey, ConsumerSecret, AccessToken, AccessTokenSecret string }

func oauthHeader(options ipOptions, method, endpoint string, form url.Values, nonce, timestamp string) (string, error) {
	params := url.Values{"oauth_consumer_key": {options.ConsumerKey}, "oauth_nonce": {nonce}, "oauth_signature_method": {"HMAC-SHA1"}, "oauth_timestamp": {timestamp}, "oauth_token": {options.AccessToken}, "oauth_version": {"1.0"}}
	base, err := oauthBaseString(method, endpoint, form, params)
	if err != nil {
		return "", err
	}
	mac := hmac.New(sha1.New, []byte(percentEncode(options.ConsumerSecret)+"&"+percentEncode(options.AccessTokenSecret)))
	_, _ = mac.Write([]byte(base))
	params.Set("oauth_signature", base64.StdEncoding.EncodeToString(mac.Sum(nil)))
	keys := make([]string, 0, len(params))
	for key := range params {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, key := range keys {
		parts = append(parts, fmt.Sprintf(`%s="%s"`, percentEncode(key), percentEncode(params.Get(key))))
	}
	return "OAuth " + strings.Join(parts, ", "), nil
}

func oauthBaseString(method, endpoint string, form, oauth url.Values) (string, error) {
	u, err := url.Parse(endpoint)
	if err != nil {
		return "", err
	}
	query := u.Query()
	u.RawQuery = ""
	u.Fragment = ""
	baseURL := u.Scheme + "://" + strings.ToLower(u.Host) + u.EscapedPath()
	params := url.Values{}
	for _, source := range []url.Values{query, form, oauth} {
		for key, values := range source {
			for _, value := range values {
				params.Add(key, value)
			}
		}
	}
	type pair struct{ key, value string }
	pairs := []pair{}
	for key, values := range params {
		for _, value := range values {
			pairs = append(pairs, pair{percentEncode(key), percentEncode(value)})
		}
	}
	sort.Slice(pairs, func(i, j int) bool {
		if pairs[i].key == pairs[j].key {
			return pairs[i].value < pairs[j].value
		}
		return pairs[i].key < pairs[j].key
	})
	encoded := make([]string, len(pairs))
	for i, pair := range pairs {
		encoded[i] = pair.key + "=" + pair.value
	}
	return strings.ToUpper(method) + "&" + percentEncode(baseURL) + "&" + percentEncode(strings.Join(encoded, "&")), nil
}
func percentEncode(value string) string {
	encoded := url.QueryEscape(value)
	encoded = strings.ReplaceAll(encoded, "+", "%20")
	return strings.ReplaceAll(encoded, "%7E", "~")
}
func oauthNonce() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}
