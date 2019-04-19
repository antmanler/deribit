package deribit

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"k8s.io/klog"

	"github.com/gorilla/websocket"
	"github.com/refunc/go-observer"
	"github.com/sourcegraph/jsonrpc2"
)

const (
	liveURL = "wss://www.deribit.com/ws/api/v2/"
	testURL = "wss://test.deribit.com/ws/api/v2/"
)

// wellknown errors
var (
	ErrAuthenticationIsRequired = errors.New("deribit: Authenticate is required")
)

// Client is client for deribit
type Client struct {
	ctx context.Context

	wsConn  *websocket.Conn
	rpcConn *jsonrpc2.Conn

	events observer.Property
	errors observer.Property

	auth struct {
		sync.RWMutex

		startOnce sync.Once
		token     string
		refresh   string
	}
}

// Event is wrapper of received event
type Event struct {
	Channel string          `json:"channel"`
	Data    json.RawMessage `json:"data"`
}

// NewClient returns a client instance, if testNet is true, it will use test net
func NewClient(ctx context.Context, testNet bool) (*Client, error) {
	var url string
	if testNet {
		url = testURL
	} else {
		url = liveURL
	}

	c, _, err := websocket.DefaultDialer.DialContext(ctx, url, nil)
	if err != nil {
		return nil, err
	}
	cli := &Client{
		ctx:    ctx,
		wsConn: c,
		events: observer.NewProperty(nil),
		errors: observer.NewProperty(nil),
	}
	cli.rpcConn = jsonrpc2.NewConn(ctx, newObjectStream(c, cli.errors), cli)
	return cli, nil
}

// Authenticate is a special remote call, it must be called brefore any "private" methods
func (cli *Client) Authenticate(accessKey, secretKey string) error {
	var result struct {
		// access token
		AccessToken string `json:"access_token"`
		// Token lifetime in seconds
		ExpiresIn int64 `json:"expires_in"`
		// Can be used to request a new token (with a new lifetime)
		RefreshToken string `json:"refresh_token"`
	}
	if err := cli.Call("public/auth", struct {
		ClientID     string `json:"client_id"`
		ClientSecret string `json:"client_secret"`
		GrantType    string `json:"grant_type"`
	}{accessKey, secretKey, "client_credentials"}, &result); err != nil {
		return err
	}

	cli.auth.startOnce.Do(func() {
		cli.auth.Lock()
		defer cli.auth.Unlock()
		cli.auth.token = result.AccessToken
		cli.auth.refresh = result.RefreshToken

		go func() {
			// Renew login 10 minutes before we have to
			const renewBefore int64 = 600
			for {
				d, err := time.ParseDuration(fmt.Sprintf("%ds", result.ExpiresIn-renewBefore))
				if err != nil {
					cli.setError(fmt.Errorf("unable to parse %ds as a duration: %s", result.ExpiresIn-renewBefore, err))
					return
				}

				klog.V(3).Infof("(deribit) refreshing token in %v", d)

				select {
				case <-time.After(d):
				case <-cli.ctx.Done():
					return
				}

				func() {
					cli.auth.Lock()
					defer cli.auth.Unlock()

					result.AccessToken = ""
					result.RefreshToken = ""
					result.ExpiresIn = 0

					if err := cli.Call("public/auth", struct {
						RefreshToken string `json:"refresh_token"`
						GrantType    string `json:"grant_type"`
					}{cli.auth.refresh, "refresh_token"}, &result); err != nil {
						cli.setError(err)
						return
					}
					cli.auth.token = result.AccessToken
					cli.auth.refresh = result.RefreshToken
				}()
			}
		}()
	})

	return nil
}

var emptyParams = json.RawMessage("{}")

// Call issues JSONRPC v2 calls
func (cli *Client) Call(method string, params interface{}, result interface{}) error {
	if params == nil {
		params = emptyParams
	}

	if token, isPrivate := params.(privateParams); isPrivate {
		if cli.auth.token == "" {
			return ErrAuthenticationIsRequired
		}
		token.setToken(cli.auth.token)
	}

	return cli.rpcConn.Call(cli.ctx, method, params, result)
}

// NewEventStream returns new events observer
func (cli *Client) NewEventStream() observer.Stream {
	return cli.events.Observe()
}

// NewErrorStream returns new errors observer
func (cli *Client) NewErrorStream() observer.Stream {
	return cli.errors.Observe()
}

// LastError returns value of error property
func (cli *Client) LastError() error {
	if err, ok := cli.errors.Value().(error); ok {
		return err
	}
	return nil
}

// Handle implements jsonrpc2.Handler
func (cli *Client) Handle(ctx context.Context, conn *jsonrpc2.Conn, req *jsonrpc2.Request) {
	if req.Method == "subscription" {
		// update events
		if req.Params != nil && len(*req.Params) > 0 {
			var event Event
			if err := json.Unmarshal(*req.Params, &event); err != nil {
				cli.setError(err)
				return
			}
			cli.events.Update(&event)
		}
	}
}

func (cli *Client) setError(err error) {
	if err != nil {
		klog.Errorf("(deribit) on error, %v", err)
		cli.errors.Update(err)
	}
}

// privateParams is interface for methods require access_token
type privateParams interface {
	setToken(token string)
}

// Token is used to embeded in params for private methods
type Token struct {
	AccessToken string `json:"access_token"`
}

func (t *Token) setToken(token string) {
	t.AccessToken = token
}
