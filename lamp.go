package lamp

import (
	"context"
	"errors"
	"net"
	"net/url"
	"os"
)

var (
	ErrNotInitialized         = errors.New("not initialized")
	ErrMiddlewareNotSupported = errors.New("middleware not supported")
	ErrAddressNotFound        = errors.New("address not found")
)

type Client interface {
	Expose(ctx context.Context, serviceName string, addrs map[string]string, ttl int64) (cancel func() error, err error)
	Discover(ctx context.Context, serviceName string, protocol string) (addrs []string, err error)
	Watch(ctx context.Context, serviceName string, protocol string, notify func(addrs []string, closed bool)) (close func(), err error)
	Close() error
}

var (
	cli Client
)

// Init
// e. etcd://127.0.0.1:2379/services?
func Init(cfg string) (close func() error, err error) {
	var URL *url.URL

	if URL, err = url.Parse(cfg); err != nil {
		panic(err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	switch URL.Scheme {
	// Use etcd
	case "etcd":
		cli, err = newEtcdWithURL(ctx, URL)
	// Not supported
	default:
		cli, err = nil, ErrMiddlewareNotSupported
	}

	if err != nil {
		cancel()
	} else {
		close = func() (err error) {
			cancel()
			if err = cli.Close(); err == nil {
				cli = nil
			}
			return
		}
	}

	return
}

type ExposeOptions struct {
	Addrs map[string]string
	TTL   int64
}

type ExposeOption func(opts *ExposeOptions)

// WithTTL
func WithTTL(ttl int64) ExposeOption {
	return func(opts *ExposeOptions) {
		opts.TTL = ttl
	}
}

// WithPublic
func WithPublic(addr string, protocol ...string) ExposeOption {
	return func(opts *ExposeOptions) {
		host, port, err := net.SplitHostPort(addr)
		if err != nil {
			return
		}
		if host == "" {
			host = os.Getenv("PUBLIC_HOSTNAME")
		}
		if host == "" || port == "" {
			return
		}
		if len(protocol) != 1 {
			protocol = []string{"any"}
		}
		opts.Addrs[protocol[0]] = host + ":" + port
	}
}

// Expose
func Expose(serviceName string, opts ...ExposeOption) (cancel func() error, err error) {
	return ExposeWithContext(context.Background(), serviceName, opts...)
}

// ExposeWithContext
func ExposeWithContext(ctx context.Context, serviceName string, opts ...ExposeOption) (cancel func() error, err error) {
	panicIfNotInited()

	expOpts := ExposeOptions{
		Addrs: make(map[string]string),
	}
	// Apply options
	for _, setOption := range opts {
		setOption(&expOpts)
	}

	// Option: TTL
	if expOpts.TTL <= 0 {
		expOpts.TTL = 30
	}

	// Option: Addrs
	if len(expOpts.Addrs) <= 0 {
		return nil, ErrAddressNotFound
	}

	return cli.Expose(ctx, serviceName, expOpts.Addrs, expOpts.TTL)
}

// Discover
func Discover(serviceName string, protocol string) (addrs []string, err error) {
	return DiscoverWithContext(context.Background(), serviceName, protocol)
}

// DiscoverWithContext
func DiscoverWithContext(ctx context.Context, serviceName string, protocol string) (addrs []string, err error) {
	panicIfNotInited()
	return cli.Discover(ctx, serviceName, protocol)
}

// Watch
func Watch(serviceName string, protocol string, notify func(addrs []string, closed bool)) (close func(), err error) {
	return WatchWithContext(context.Background(), serviceName, protocol, notify)
}

// WatchWithContext
func WatchWithContext(ctx context.Context, serviceName string, protocol string, notify func(addrs []string, closed bool)) (close func(), err error) {
	panicIfNotInited()
	return cli.Watch(ctx, serviceName, protocol, notify)
}

// panicIfNotInited
func panicIfNotInited() {
	if cli == nil {
		panic(ErrNotInitialized)
	}
}
