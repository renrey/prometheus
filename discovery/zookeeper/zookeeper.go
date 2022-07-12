// Copyright 2015 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package zookeeper

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/go-kit/log"
	"github.com/go-zookeeper/zk"
	"github.com/prometheus/common/model"

	"github.com/prometheus/prometheus/discovery"
	"github.com/prometheus/prometheus/discovery/targetgroup"
	"github.com/prometheus/prometheus/util/strutil"
	"github.com/prometheus/prometheus/util/treecache"
)

var (
	// DefaultServersetSDConfig is the default Serverset SD configuration.
	DefaultServersetSDConfig = ServersetSDConfig{
		Timeout: model.Duration(10 * time.Second),
	}
	// DefaultNerveSDConfig is the default Nerve SD configuration.
	DefaultNerveSDConfig = NerveSDConfig{
		Timeout: model.Duration(10 * time.Second),
	}
)

func init() {
	discovery.RegisterConfig(&ServersetSDConfig{})
	discovery.RegisterConfig(&NerveSDConfig{})
}

// ServersetSDConfig is the configuration for Twitter serversets in Zookeeper based discovery.
type ServersetSDConfig struct {
	Servers []string       `yaml:"servers"`
	Paths   []string       `yaml:"paths"`
	Timeout model.Duration `yaml:"timeout,omitempty"`
}

// Name returns the name of the Config.
func (*ServersetSDConfig) Name() string { return "serverset" }

// NewDiscoverer returns a Discoverer for the Config.
func (c *ServersetSDConfig) NewDiscoverer(opts discovery.DiscovererOptions) (discovery.Discoverer, error) {
	return NewServersetDiscovery(c, opts.Logger)
}

// UnmarshalYAML implements the yaml.Unmarshaler interface.
func (c *ServersetSDConfig) UnmarshalYAML(unmarshal func(interface{}) error) error {
	*c = DefaultServersetSDConfig
	type plain ServersetSDConfig
	err := unmarshal((*plain)(c))
	if err != nil {
		return err
	}
	if len(c.Servers) == 0 {
		return errors.New("serverset SD config must contain at least one Zookeeper server")
	}
	if len(c.Paths) == 0 {
		return errors.New("serverset SD config must contain at least one path")
	}
	for _, path := range c.Paths {
		if !strings.HasPrefix(path, "/") {
			return fmt.Errorf("serverset SD config paths must begin with '/': %s", path)
		}
	}
	return nil
}

// NerveSDConfig is the configuration for AirBnB's Nerve in Zookeeper based discovery.
type NerveSDConfig struct {
	Servers []string       `yaml:"servers"`
	Paths   []string       `yaml:"paths"`
	Timeout model.Duration `yaml:"timeout,omitempty"`
}

// Name returns the name of the Config.
func (*NerveSDConfig) Name() string { return "nerve" }

// NewDiscoverer returns a Discoverer for the Config.
func (c *NerveSDConfig) NewDiscoverer(opts discovery.DiscovererOptions) (discovery.Discoverer, error) {
	return NewNerveDiscovery(c, opts.Logger)
}

// UnmarshalYAML implements the yaml.Unmarshaler interface.
func (c *NerveSDConfig) UnmarshalYAML(unmarshal func(interface{}) error) error {
	*c = DefaultNerveSDConfig
	type plain NerveSDConfig
	err := unmarshal((*plain)(c))
	if err != nil {
		return err
	}
	if len(c.Servers) == 0 {
		return errors.New("nerve SD config must contain at least one Zookeeper server")
	}
	if len(c.Paths) == 0 {
		return errors.New("nerve SD config must contain at least one path")
	}
	for _, path := range c.Paths {
		if !strings.HasPrefix(path, "/") {
			return fmt.Errorf("nerve SD config paths must begin with '/': %s", path)
		}
	}
	return nil
}

// Discovery implements the Discoverer interface for discovering
// targets from Zookeeper.
type Discovery struct {
	conn *zk.Conn

	sources map[string]*targetgroup.Group

	updates     chan treecache.ZookeeperTreeCacheEvent
	pathUpdates []chan treecache.ZookeeperTreeCacheEvent
	treeCaches  []*treecache.ZookeeperTreeCache

	parse  func(data []byte, path string) (model.LabelSet, error)
	logger log.Logger
}

// NewNerveDiscovery returns a new Discovery for the given Nerve config.
func NewNerveDiscovery(conf *NerveSDConfig, logger log.Logger) (*Discovery, error) {
	return NewDiscovery(conf.Servers, time.Duration(conf.Timeout), conf.Paths, logger, parseNerveMember)
}

// NewServersetDiscovery returns a new Discovery for the given serverset config.
func NewServersetDiscovery(conf *ServersetSDConfig, logger log.Logger) (*Discovery, error) {
	return NewDiscovery(conf.Servers, time.Duration(conf.Timeout), conf.Paths, logger, parseServersetMember)
}

// NewDiscovery returns a new discovery along Zookeeper parses with
// the given parse function.
func NewDiscovery(
	srvs []string,
	timeout time.Duration,
	paths []string,
	logger log.Logger,
	pf func(data []byte, path string) (model.LabelSet, error),
) (*Discovery, error) {
	if logger == nil {
		logger = log.NewNopLogger()
	}

	// 1. 创建zk连接
	conn, _, err := zk.Connect(
		srvs, timeout,
		func(c *zk.Conn) {
			c.SetLogger(treecache.NewZookeeperLogger(logger))
		})
	if err != nil {
		return nil, err
	}
	// 2. 创建channel
	updates := make(chan treecache.ZookeeperTreeCacheEvent)
	// 3， 创建sd对象
	sd := &Discovery{
		conn:    conn,
		updates: updates,
		sources: map[string]*targetgroup.Group{},
		parse:   pf,
		logger:  logger,
	}
	// 4. 需要监听的path缓存（包含里面的子节点）
	for _, path := range paths {
		pathUpdate := make(chan treecache.ZookeeperTreeCacheEvent)                                             // 也是channel
		sd.pathUpdates = append(sd.pathUpdates, pathUpdate)                                                    // 保存channel
		sd.treeCaches = append(sd.treeCaches, treecache.NewZookeeperTreeCache(conn, path, pathUpdate, logger)) // 缓存对象
	}
	return sd, nil
}

// Run implements the Discoverer interface.
func (d *Discovery) Run(ctx context.Context, ch chan<- []*targetgroup.Group) {
	// 关闭时执行
	defer func() {
		for _, tc := range d.treeCaches {
			tc.Stop()
		}
		for _, pathUpdate := range d.pathUpdates {
			// Drain event channel in case the treecache leaks goroutines otherwise.
			for range pathUpdate {
			}
		}
		d.conn.Close()
	}()

	// 遍历所有需要监听的path缓存对应channel
	for _, pathUpdate := range d.pathUpdates {
		// 启动新的goruntime，监听对于缓存（树，包含子节点）是否有变更，有变更就交给updates
		go func(update chan treecache.ZookeeperTreeCacheEvent) {
			// 有变更
			for event := range update {
				select {
				case d.updates <- event: // 变更交给updates
				case <-ctx.Done():
					return
				}
			}
		}(pathUpdate)
	}

	for {
		select {
		case <-ctx.Done():
			return
		case event := <-d.updates: // 处理上面的那些树缓存监听到的变更
			// 创建新的tg，source=path
			tg := &targetgroup.Group{
				Source: event.Path,
			}
			if event.Data != nil {
				// data转换成labelset
				labelSet, err := d.parse(*event.Data, event.Path)
				if err == nil {
					// targets直接只有当前labelSet
					tg.Targets = []model.LabelSet{labelSet}
					d.sources[event.Path] = tg // 当前sd客户端的source保存对应path的tg
				} else {
					delete(d.sources, event.Path)
				}
			} else {
				delete(d.sources, event.Path)
			}
			select {
			case <-ctx.Done():
				return
			case ch <- []*targetgroup.Group{tg}: // 返回单个新tg
			}
		}
	}
}

const (
	serversetLabelPrefix         = model.MetaLabelPrefix + "serverset_"
	serversetStatusLabel         = serversetLabelPrefix + "status"
	serversetPathLabel           = serversetLabelPrefix + "path"
	serversetEndpointLabelPrefix = serversetLabelPrefix + "endpoint"
	serversetShardLabel          = serversetLabelPrefix + "shard"
)

type serversetMember struct {
	ServiceEndpoint     serversetEndpoint
	AdditionalEndpoints map[string]serversetEndpoint
	Status              string `json:"status"`
	Shard               int    `json:"shard"`
}

type serversetEndpoint struct {
	Host string
	Port int
}

func parseServersetMember(data []byte, path string) (model.LabelSet, error) {
	member := serversetMember{}

	if err := json.Unmarshal(data, &member); err != nil {
		return nil, fmt.Errorf("error unmarshaling serverset member %q: %w", path, err)
	}

	labels := model.LabelSet{}
	labels[serversetPathLabel] = model.LabelValue(path)
	labels[model.AddressLabel] = model.LabelValue(
		net.JoinHostPort(member.ServiceEndpoint.Host, fmt.Sprintf("%d", member.ServiceEndpoint.Port)))

	labels[serversetEndpointLabelPrefix+"_host"] = model.LabelValue(member.ServiceEndpoint.Host)
	labels[serversetEndpointLabelPrefix+"_port"] = model.LabelValue(fmt.Sprintf("%d", member.ServiceEndpoint.Port))

	for name, endpoint := range member.AdditionalEndpoints {
		cleanName := model.LabelName(strutil.SanitizeLabelName(name))
		labels[serversetEndpointLabelPrefix+"_host_"+cleanName] = model.LabelValue(
			endpoint.Host)
		labels[serversetEndpointLabelPrefix+"_port_"+cleanName] = model.LabelValue(
			fmt.Sprintf("%d", endpoint.Port))

	}

	labels[serversetStatusLabel] = model.LabelValue(member.Status)
	labels[serversetShardLabel] = model.LabelValue(strconv.Itoa(member.Shard))

	return labels, nil
}

const (
	nerveLabelPrefix         = model.MetaLabelPrefix + "nerve_"
	nervePathLabel           = nerveLabelPrefix + "path"
	nerveEndpointLabelPrefix = nerveLabelPrefix + "endpoint"
)

type nerveMember struct {
	Host string `json:"host"`
	Port int    `json:"port"`
	Name string `json:"name"`
}

func parseNerveMember(data []byte, path string) (model.LabelSet, error) {
	member := nerveMember{}
	err := json.Unmarshal(data, &member)
	if err != nil {
		return nil, fmt.Errorf("error unmarshaling nerve member %q: %w", path, err)
	}

	labels := model.LabelSet{}
	labels[nervePathLabel] = model.LabelValue(path)
	labels[model.AddressLabel] = model.LabelValue(
		net.JoinHostPort(member.Host, fmt.Sprintf("%d", member.Port)))

	labels[nerveEndpointLabelPrefix+"_host"] = model.LabelValue(member.Host)
	labels[nerveEndpointLabelPrefix+"_port"] = model.LabelValue(fmt.Sprintf("%d", member.Port))
	labels[nerveEndpointLabelPrefix+"_name"] = model.LabelValue(member.Name)

	return labels, nil
}
