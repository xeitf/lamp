package lamp

import (
	"context"
	"errors"
	"net/url"
)

var (
	ErrMiddlewareNotSupported = errors.New("middleware not supported")
)

type Client interface {
	Register(ctx context.Context, serviceName string, addrs map[string]string, ttl int64) (cancel func() error, err error)
	Discover(ctx context.Context, serviceName string, protocol string) (addrs []string, err error)
	Watch(ctx context.Context, serviceName string, protocol string, notify func(addrs []string, closed bool)) (close func(), err error)
	Close() error
}

var cli Client

// Init
// e. etcd://127.0.0.1:2379/services?
func Init(cfg string) (close func() error, err error) {
	var u *url.URL

	if u, err = url.Parse(cfg); err != nil {
		panic(err)
	}

	if u.Scheme != "etcd" {
		return nil, ErrMiddlewareNotSupported
	}

	cli, err = newEtcdWithURL(u)
	if err == nil {
		close = func() error { return cli.Close() }
	}
	return
}

type RegisterOptions struct {
	Addrs map[string]string
	TTL   int64
}

type RegisterOption func(opts *RegisterOptions)

// WithTTL
func WithTTL(ttl int64) RegisterOption {
	return func(opts *RegisterOptions) {
		opts.TTL = ttl
	}
}

// WithPublic
func WithPublic(addr string, protocol ...string) RegisterOption {
	return func(opts *RegisterOptions) {
		if len(protocol) != 1 {
			protocol = []string{"any"}
		}
		opts.Addrs[protocol[0]] = addr
	}
}

// Register
func Register(serviceName string, opts ...RegisterOption) (cancel func() error, err error) {
	return RegisterWithContext(context.Background(), serviceName, opts...)
}

// RegisterWithContext
func RegisterWithContext(ctx context.Context, serviceName string, opts ...RegisterOption) (cancel func() error, err error) {
	regOpts := RegisterOptions{
		Addrs: make(map[string]string),
	}
	// Apply options
	for _, setOption := range opts {
		setOption(&regOpts)
	}

	// Option: TTL
	if regOpts.TTL <= 0 {
		regOpts.TTL = 30
	}

	return cli.Register(ctx, serviceName, regOpts.Addrs, regOpts.TTL)
}

// Discover
func Discover(serviceName string, protocol string) (addrs []string, err error) {
	return DiscoverWithContext(context.Background(), serviceName, protocol)
}

// DiscoverWithContext
func DiscoverWithContext(ctx context.Context, serviceName string, protocol string) (addrs []string, err error) {
	return cli.Discover(ctx, serviceName, protocol)
}

// Watch
func Watch(serviceName string, protocol string, notify func(addrs []string, closed bool)) (close func(), err error) {
	return WatchWithContext(context.Background(), serviceName, protocol, notify)
}

// WatchWithContext
func WatchWithContext(ctx context.Context, serviceName string, protocol string, notify func(addrs []string, closed bool)) (close func(), err error) {
	return cli.Watch(ctx, serviceName, protocol, notify)
}
