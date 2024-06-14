package lamp

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/url"
	"strings"
	"sync"
	"time"

	client "go.etcd.io/etcd/client/v3"
)

type etcdConfig struct {
	client.Config
	Namespace string
}

type etcdClient struct {
	cfg etcdConfig
	cli *client.Client
}

var (
	ErrInvalidEndpoint = errors.New("invalid endpoint")
	ErrAddressNotFound = errors.New("address not found")
	ErrRegisterFailed  = errors.New("register failed")
)

// newEtcd
func newEtcd(cfg etcdConfig) (c *etcdClient, err error) {
	cli, err := client.New(cfg.Config)
	if err != nil {
		return nil, err
	}
	c = &etcdClient{cfg: cfg, cli: cli}
	return
}

// newEtcdWithURL
func newEtcdWithURL(u *url.URL) (c *etcdClient, err error) {
	var cfg etcdConfig

	// Option: Endpoints
	if u.Host == "" {
		return nil, ErrInvalidEndpoint
	}
	for _, endpoint := range strings.Split(u.Host, ",") {
		if _, _, err = net.SplitHostPort(endpoint); err != nil {
			return nil, err
		}
		cfg.Endpoints = append(cfg.Endpoints, endpoint)
	}

	// Option: Namespace
	if u.Path != "" && u.Path != "/" {
		cfg.Namespace = u.Path
	}

	return newEtcd(cfg)
}

// Close shuts down the client's etcd connections.
func (c *etcdClient) Close() error {
	return c.cli.Close()
}

// Register
func (c *etcdClient) Register(ctx context.Context, serviceName string, addrs map[string]string, ttl int64) (cancel func() error, err error) {
	if len(addrs) <= 0 {
		return nil, ErrAddressNotFound
	}

	leaseResp, err := c.cli.Grant(ctx, int64(ttl))
	if err != nil {
		return nil, err
	}

	leaseKeepAliveRespChan, err := c.cli.KeepAlive(ctx, leaseResp.ID)
	if err != nil {
		return nil, err
	}

	ctx, cancelCtx := context.WithCancel(ctx)

	cancel = func() (err error) {
		cancelCtx()
		_, err = c.cli.Revoke(ctx, leaseResp.ID)
		return
	}

	var wg sync.WaitGroup
	var now = time.Now()
	var ops = make([]client.Op, 0, len(addrs))
	var servicePrefix = c.cfg.Namespace + "/" + serviceName

	wg.Add(1)
	go func() {
		wg.Done()
		defer cancel()
		for {
			select {
			// KeepAlive response
			case _, ok := <-leaseKeepAliveRespChan:
				if !ok {
					return
				}
			// Cancel
			case <-ctx.Done():
				return
			}
		}
	}()

	wg.Wait()

	for protocol, addr := range addrs {
		node := Node{
			Addr: addr,
			Time: now.Unix(),
		}
		info, err := json.Marshal(node)
		if err != nil {
			cancel()
			return nil, err
		}
		ops = append(ops,
			client.OpPut(
				servicePrefix+"/"+protocol+"/"+addr,
				string(info),
				client.WithLease(leaseResp.ID),
			))
	}

	txnResp, err := c.cli.Txn(ctx).If().Then(ops...).Commit()
	if err != nil {
		defer cancel()
		return nil, err
	}

	if !txnResp.Succeeded {
		defer cancel()
		return nil, ErrRegisterFailed
	}

	return
}

func (c *etcdClient) Discover(ctx context.Context, serviceName string, protocol string) (addrs []string, err error) {
	servicePrefix := c.cfg.Namespace + "/" + serviceName

	getResp, err := c.cli.Get(ctx, servicePrefix, client.WithPrefix())
	if err != nil {
		return nil, err
	}

	protocolAddrs := make(map[string][]string)

	for _, kv := range getResp.Kvs {
		protocolAndNodeID := strings.Split(
			strings.TrimPrefix(string(kv.Key), servicePrefix+"/"), "/")
		if len(protocolAndNodeID) != 2 {
			continue
		}
		var node Node
		var prot = protocolAndNodeID[0]
		if err := json.Unmarshal(kv.Value, &node); err != nil {
			continue
		}
		if _, ok := protocolAddrs[prot]; !ok {
			protocolAddrs[prot] = []string{node.Addr}
		} else {
			protocolAddrs[prot] = append(protocolAddrs[prot], node.Addr)
		}
	}

	if addrs, ok := protocolAddrs[protocol]; ok {
		return addrs, nil
	} else if addrs, ok := protocolAddrs["any"]; ok {
		return addrs, nil
	}

	return nil, nil
}

func (c *etcdClient) Watch(ctx context.Context, serviceName string, protocol string, notify func(addrs []string, closed bool)) (close func(), err error) {
	var wg sync.WaitGroup
	var serviceAddrs = make(map[string]Node)
	var servicePrefix = c.cfg.Namespace + "/" + serviceName

	getResp, err := c.cli.Get(ctx, servicePrefix, client.WithPrefix())
	if err != nil {
		return nil, err
	}

	for _, kv := range getResp.Kvs {
		protocolAndNodeID := strings.TrimPrefix(string(kv.Key), servicePrefix+"/")
		var node Node
		if err := json.Unmarshal(kv.Value, &node); err != nil {
			continue
		}
		serviceAddrs[protocolAndNodeID] = node
	}

	notify(c.selectServiceAddrs(serviceAddrs, protocol), false)

	ctx, close = context.WithCancel(ctx)
	watchChan := c.cli.Watch(ctx, servicePrefix, client.WithPrefix())

	wg.Add(1)
	go func() {
		wg.Done()
		defer notify(nil, true)
		for {
			select {
			case watchResp, ok := <-watchChan:
				if !ok {
					return
				}
				for _, event := range watchResp.Events {
					var node Node
					if err := json.Unmarshal(event.Kv.Value, &node); err != nil {
						continue
					}
					var protocolAndNodeID = strings.TrimPrefix(string(event.Kv.Key), servicePrefix+"/")

					switch event.Type {
					case client.EventTypePut:
						serviceAddrs[protocolAndNodeID] = node
					case client.EventTypeDelete:
						delete(serviceAddrs, protocolAndNodeID)
					}
				}

				notify(c.selectServiceAddrs(serviceAddrs, protocol), false)
			}
		}

	}()

	wg.Wait()

	return
}

func (c *etcdClient) selectServiceAddrs(serviceAddrs map[string]Node, protocol string) (addrs []string) {
	protocols := make(map[string][]string)
	for protocolAndNodeID, node := range serviceAddrs {
		if protocol != "any" &&
			strings.HasPrefix(protocolAndNodeID, protocol+"/") {
			if _, ok := protocols[protocol]; !ok {
				protocols[protocol] = []string{node.Addr}
			} else {
				protocols[protocol] = append(protocols[protocol], node.Addr)
			}
		}
		if strings.HasPrefix(protocolAndNodeID, "any/") {
			if _, ok := protocols["any"]; !ok {
				protocols["any"] = []string{node.Addr}
			} else {
				protocols["any"] = append(protocols[protocol], node.Addr)
			}
		}
	}
	if addrs, ok := protocols[protocol]; ok {
		return addrs
	} else if addrs, ok := protocols["any"]; ok {
		return addrs
	}
	return nil
}
