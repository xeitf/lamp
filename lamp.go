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

type Middleware interface {
	Expose(ctx context.Context, serviceName string, addrs map[string]string, ttl int64) (cancel func() error, err error)
	Discover(ctx context.Context, serviceName string, protocol string) (addrs []string, err error)
	Watch(ctx context.Context, serviceName string, protocol string, notify func(addrs []string, closed bool)) (close func(), err error)
	Close() error
}

type Client struct {
	middleware Middleware
	close      func() error
}

// NewCLient
// e. etcd://127.0.0.1:2379/services?
func NewClient(cfg string) (cli *Client, err error) {
	var URL *url.URL
	var middleware Middleware

	if URL, err = url.Parse(cfg); err != nil {
		return
	}

	ctx, cancelCtx := context.WithCancel(context.Background())

	switch URL.Scheme {
	// Use etcd
	case "etcd":
		middleware, err = newEtcdWithURL(ctx, URL)
	// Not supported
	default:
		err = ErrMiddlewareNotSupported
	}

	if err != nil {
		cancelCtx()
		return
	}

	cli = &Client{middleware: middleware}
	cli.close = func() (err error) {
		cancelCtx()
		return middleware.Close()
	}

	return
}

type exposeOptions struct {
	Addrs map[string]string
	TTL   int64
}

type fnExposeOption struct {
	f func(*exposeOptions)
}

// newFnExposeOption
func newFnExposeOption(f func(*exposeOptions)) *fnExposeOption {
	return &fnExposeOption{f: f}
}

// apply
func (expOpt *fnExposeOption) apply(opts *exposeOptions) {
	expOpt.f(opts)
}

type ExposeOption interface {
	apply(opts *exposeOptions)
}

// WithTTL
func WithTTL(ttl int64) ExposeOption {
	return newFnExposeOption(func(opts *exposeOptions) { opts.TTL = ttl })
}

// WithPublic
func WithPublic(addr string, protocol ...string) ExposeOption {
	return newFnExposeOption(func(opts *exposeOptions) {
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
	})
}

// Expose
func (cli *Client) Expose(serviceName string, opts ...ExposeOption) (cancel func() error, err error) {
	return cli.ExposeWithContext(context.Background(), serviceName, opts...)
}

// ExposeWithContext
func (cli *Client) ExposeWithContext(ctx context.Context, serviceName string, opts ...ExposeOption) (cancel func() error, err error) {
	expOpts := exposeOptions{
		Addrs: make(map[string]string),
	}
	// Apply options
	for _, opt := range opts {
		opt.apply(&expOpts)
	}

	// Option: TTL
	if expOpts.TTL <= 0 {
		expOpts.TTL = 30
	}

	// Option: Addrs
	if len(expOpts.Addrs) <= 0 {
		return nil, ErrAddressNotFound
	}

	return cli.middleware.Expose(ctx, serviceName, expOpts.Addrs, expOpts.TTL)
}

// Discover
func (cli *Client) Discover(serviceName string, protocol string) (addrs []string, err error) {
	return cli.DiscoverWithContext(context.Background(), serviceName, protocol)
}

// DiscoverWithContext
func (cli *Client) DiscoverWithContext(ctx context.Context, serviceName string, protocol string) (addrs []string, err error) {
	return cli.middleware.Discover(ctx, serviceName, protocol)
}

// Watch
func (cli *Client) Watch(serviceName string, protocol string, notify func(addrs []string, closed bool)) (close func(), err error) {
	return cli.WatchWithContext(context.Background(), serviceName, protocol, notify)
}

// WatchWithContext
func (cli *Client) WatchWithContext(ctx context.Context, serviceName string, protocol string, notify func(addrs []string, closed bool)) (close func(), err error) {
	return cli.middleware.Watch(ctx, serviceName, protocol, notify)
}

// Close
func (cli *Client) Close() (err error) {
	return cli.close()
}
