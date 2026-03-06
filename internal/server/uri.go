package server

import (
	"errors"
	"fmt"
	"net"
	"net/url"
	"strconv"
	"strings"
)

type Endpoint struct {
	Token   string
	Address string
}

func ParseURI(raw string) (Endpoint, error) {
	parsed, err := url.Parse(raw)
	if err != nil {
		return Endpoint{}, fmt.Errorf("parse region URI: %w", err)
	}
	if parsed.Scheme != "region" {
		return Endpoint{}, errors.New("parse region URI: scheme must be region")
	}
	if parsed.Opaque != "" || parsed.Path != "/" || parsed.RawQuery != "" ||
		parsed.ForceQuery || parsed.Fragment != "" {
		return Endpoint{}, errors.New("parse region URI: URI must end at the root path")
	}
	if parsed.User == nil {
		return Endpoint{}, errors.New("parse region URI: token is required")
	}
	if _, hasPassword := parsed.User.Password(); hasPassword {
		return Endpoint{}, errors.New("parse region URI: password syntax is not supported")
	}
	token := parsed.User.Username()
	if err := validateToken(token); err != nil {
		return Endpoint{}, err
	}

	host := parsed.Hostname()
	port := parsed.Port()
	if host == "" {
		return Endpoint{}, errors.New("parse region URI: host is required")
	}
	if strings.Count(parsed.Host, ":") > 1 && !strings.HasPrefix(parsed.Host, "[") {
		return Endpoint{}, errors.New("parse region URI: IPv6 hosts must use brackets")
	}
	if port == "" {
		return Endpoint{}, errors.New("parse region URI: port is required")
	}
	portNumber, err := strconv.ParseUint(port, 10, 16)
	if err != nil || portNumber == 0 {
		return Endpoint{}, errors.New("parse region URI: port must be between 1 and 65535")
	}

	return Endpoint{Token: token, Address: net.JoinHostPort(host, port)}, nil
}

func validateToken(token string) error {
	if token == "" {
		return errors.New("parse region URI: token is required")
	}
	for _, character := range []byte(token) {
		if character < 0x21 || character > 0x7e {
			return errors.New("parse region URI: token must be printable ASCII without spaces")
		}
	}
	return nil
}
