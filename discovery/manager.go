// Copyright 2016 The Prometheus Authors
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

package discovery

import (
	"context"
	"fmt"
	"reflect"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/config"

	"github.com/prometheus/prometheus/discovery/targetgroup"
)

var (
	failedConfigs = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "prometheus_sd_failed_configs",
			Help: "Current number of service discovery configurations that failed to load.",
		},
		[]string{"name"},
	)
	discoveredTargets = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "prometheus_sd_discovered_targets",
			Help: "Current number of discovered targets.",
		},
		[]string{"name", "config"},
	)
	receivedUpdates = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "prometheus_sd_received_updates_total",
			Help: "Total number of update events received from the SD providers.",
		},
		[]string{"name"},
	)
	delayedUpdates = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "prometheus_sd_updates_delayed_total",
			Help: "Total number of update events that couldn't be sent immediately.",
		},
		[]string{"name"},
	)
	sentUpdates = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "prometheus_sd_updates_total",
			Help: "Total number of update events sent to the SD consumers.",
		},
		[]string{"name"},
	)
)

func RegisterMetrics() {
	prometheus.MustRegister(failedConfigs, discoveredTargets, receivedUpdates, delayedUpdates, sentUpdates)
}

type poolKey struct {
	setName  string
	provider string
}

// Provider holds a Discoverer instance, its configuration, cancel func and its subscribers.
type Provider struct {
	name   string
	d      Discoverer
	config interface{}

	cancel context.CancelFunc
	// done should be called after cleaning up resources associated with cancelled provider.
	done func()

	mu   sync.RWMutex
	subs map[string]struct{}

	// newSubs is used to temporary store subs to be used upon config reload completion.
	newSubs map[string]struct{}
}

// Discoverer return the Discoverer of the provider
func (p *Provider) Discoverer() Discoverer {
	return p.d
}

// IsStarted return true if Discoverer is started.
func (p *Provider) IsStarted() bool {
	return p.cancel != nil
}

func (p *Provider) Config() interface{} {
	return p.config
}

// NewManager is the Discovery Manager constructor.
func NewManager(ctx context.Context, logger log.Logger, options ...func(*Manager)) *Manager {
	if logger == nil {
		logger = log.NewNopLogger()
	}
	mgr := &Manager{
		logger:      logger,
		syncCh:      make(chan map[string][]*targetgroup.Group),
		targets:     make(map[poolKey]map[string]*targetgroup.Group),
		ctx:         ctx,
		updatert:    5 * time.Second, //默认5s定时
		triggerSend: make(chan struct{}, 1),
	}
	for _, option := range options {
		option(mgr)
	}
	return mgr
}

// Name sets the name of the manager.
func Name(n string) func(*Manager) {
	return func(m *Manager) {
		m.mtx.Lock()
		defer m.mtx.Unlock()
		m.name = n
	}
}

// HTTPClientOptions sets the list of HTTP client options to expose to
// Discoverers. It is up to Discoverers to choose to use the options provided.
func HTTPClientOptions(opts ...config.HTTPClientOption) func(*Manager) {
	return func(m *Manager) {
		m.httpOpts = opts
	}
}

// Manager maintains a set of discovery providers and sends each update to a map channel.
// Targets are grouped by the target set name.
type Manager struct {
	logger   log.Logger
	name     string
	httpOpts []config.HTTPClientOption
	mtx      sync.RWMutex
	ctx      context.Context

	// Some Discoverers(e.g. k8s) send only the updates for a given target group,
	// so we use map[tg.Source]*targetgroup.Group to know which group to update.
	// 存储所有target 保存的map：key：集合名，value：map(详细名，对象)
	// 1. k：jobname + sd类型名, v ：当前下所有对象
	// 第二级 k：二级分类名，如zk类型为path，而eureka无二级分类就叫eureka， v：下面的实例集合
	targets    map[poolKey]map[string]*targetgroup.Group
	targetsMtx sync.Mutex

	// providers keeps track of SD providers.
	providers []*Provider
	// The sync channel sends the updates as a map where the key is the job value from the scrape config.
	// 从scrape获取得的配置，用于发送给其他地方
	syncCh chan map[string][]*targetgroup.Group

	// How long to wait before sending updates to the channel. The variable
	// should only be modified in unit tests.
	// 发送新的变更到channel的最少间隔
	updatert time.Duration

	// The triggerSend channel signals to the Manager that new updates have been received from providers.
	// 接受provider有处理是否有处理新的变更
	triggerSend chan struct{}

	// lastProvider counts providers registered during Manager's lifetime.
	lastProvider uint
}

// Providers returns the currently configured SD providers.
func (m *Manager) Providers() []*Provider {
	return m.providers
}

// Run starts the background processing.
func (m *Manager) Run() error {
	// 后台异步，运行sender()
	go m.sender()
	// 阻塞直到channel有东西进去，就是要关闭了才会唤醒
	for range m.ctx.Done() {
		m.cancelDiscoverers()
		return m.ctx.Err()
	}
	return nil
}

// SyncCh returns a read only channel used by all the clients to receive target updates.
// 一个返回实际target变更的channel
func (m *Manager) SyncCh() <-chan map[string][]*targetgroup.Group {
	return m.syncCh
}

// 主要作用就是创建yaml配置中target服务发现的provider
// 校验当前配置的provider是否正常运行并启动 ，不是则重新启动
// 一般全局初始化执行1次
// ApplyConfig checks if discovery provider with supplied config is already running and keeps them as is.
// Remaining providers are then stopped and new required providers are started using the provided config.
func (m *Manager) ApplyConfig(cfg map[string]Configs) error {
	m.mtx.Lock()
	defer m.mtx.Unlock()

	var failedCount int
	// 遍历sd配置数组，注册所有provider（创建）name=jobname
	// job里的每个sd配置（不只是每种），都注册一个provider
	for name, scfg := range cfg {
		failedCount += m.registerProviders(scfg, name)
	}
	failedConfigs.WithLabelValues(m.name).Set(float64(failedCount))

	var (
		wg sync.WaitGroup
		// keep shows if we keep any providers after reload.
		keep         bool
		newProviders []*Provider
	)
	// 遍历provider并启动每一个provider
	for _, prov := range m.providers {
		// Cancel obsolete providers.
		if len(prov.newSubs) == 0 {
			wg.Add(1)
			// 定义取消函数：唤醒下面wg.wait阻塞
			prov.done = func() {
				wg.Done()
			}
			prov.cancel()
			continue
		}
		// 加入到newProviders
		newProviders = append(newProviders, prov)
		// refTargets keeps reference targets used to populate new subs' targets
		var refTargets map[string]*targetgroup.Group
		prov.mu.Lock()

		m.targetsMtx.Lock()
		for s := range prov.subs {
			keep = true
			refTargets = m.targets[poolKey{s, prov.name}]
			// Remove obsolete subs' targets.
			if _, ok := prov.newSubs[s]; !ok {
				delete(m.targets, poolKey{s, prov.name})
				discoveredTargets.DeleteLabelValues(m.name, s)
			}
		}
		// Set metrics and targets for new subs.
		// 初始就是jobname的key的空map
		for s := range prov.newSubs {
			if _, ok := prov.subs[s]; !ok {
				discoveredTargets.WithLabelValues(m.name, s).Set(0)
			}
			if l := len(refTargets); l > 0 {
				m.targets[poolKey{s, prov.name}] = make(map[string]*targetgroup.Group, l)
				for k, v := range refTargets {
					m.targets[poolKey{s, prov.name}][k] = v
				}
			}
		}
		m.targetsMtx.Unlock()

		// newSubs更新到subs（正式了），并清掉new的
		prov.subs = prov.newSubs
		prov.newSubs = map[string]struct{}{}
		prov.mu.Unlock()
		// 启动provider！！！！！！
		if !prov.IsStarted() {
			m.startProvider(m.ctx, prov)
		}
	}
	// Currently downstream managers expect full target state upon config reload, so we must oblige.
	// While startProvider does pull the trigger, it may take some time to do so, therefore
	// we pull the trigger as soon as possible so that downstream managers can populate their state.
	// See https://github.com/prometheus/prometheus/pull/8639 for details.
	if keep {
		select {
		case m.triggerSend <- struct{}{}:
		default:
		}
	}
	m.providers = newProviders
	wg.Wait() // 阻塞等待，直到上面取消函数唤醒

	return nil
}

// StartCustomProvider is used for sdtool. Only use this if you know what you're doing.
func (m *Manager) StartCustomProvider(ctx context.Context, name string, worker Discoverer) {
	p := &Provider{
		name: name,
		d:    worker,
		subs: map[string]struct{}{
			name: {},
		},
	}
	m.providers = append(m.providers, p)
	m.startProvider(ctx, p)
}

func (m *Manager) startProvider(ctx context.Context, p *Provider) {
	level.Debug(m.logger).Log("msg", "Starting provider", "provider", p.name, "subs", fmt.Sprintf("%v", p.subs))
	ctx, cancel := context.WithCancel(ctx)
	updates := make(chan []*targetgroup.Group)

	p.cancel = cancel

	// 启动线程：获取信息变更
	// 定时刷新信息（发现），放入updates中
	go p.d.Run(ctx, updates)
	// 启动线程：获取上面的变更，执行处理
	// 从udpates获取更新，执行更新
	go m.updater(ctx, p, updates)
}

// cleaner cleans resources associated with provider.
func (m *Manager) cleaner(p *Provider) {
	m.targetsMtx.Lock()
	p.mu.RLock()
	for s := range p.subs {
		delete(m.targets, poolKey{s, p.name})
	}
	p.mu.RUnlock()
	m.targetsMtx.Unlock()
	if p.done != nil {
		p.done()
	}
}

func (m *Manager) updater(ctx context.Context, p *Provider, updates chan []*targetgroup.Group) {
	// Ensure targets from this provider are cleaned up.
	defer m.cleaner(p)
	for {
		select {
		case <-ctx.Done():
			return
		case tgs, ok := <-updates: // 有更新
			receivedUpdates.WithLabelValues(m.name).Inc()
			if !ok {
				level.Debug(m.logger).Log("msg", "Discoverer channel closed", "provider", p.name)
				// Wait for provider cancellation to ensure targets are cleaned up when expected.
				<-ctx.Done()
				return
			}

			p.mu.RLock()
			// 遍历p当前的subs，s：job，就是当前provider所订阅的job
			for s := range p.subs {
				// 把数据更新manager的target中，就是更新当前job+当前sd类型的target数据
				m.updateGroup(poolKey{setName: s, provider: p.name}, tgs)
			}
			p.mu.RUnlock()

			select {
			case m.triggerSend <- struct{}{}: // 写入空的到triggerSend，告诉manager本身已经更新成功
			default:
			}
		}
	}
}

func (m *Manager) sender() {
	// 启动计时器5s
	ticker := time.NewTicker(m.updatert)
	defer ticker.Stop()

	for {
		select {
		case <-m.ctx.Done(): // 要关闭了，结束
			return
			// ticker.C有东西，就是ticker到间隔了
		case <-ticker.C: // Some discoverers send updates too often, so we throttle these with the ticker.
			select {
			case <-m.triggerSend: // 间隔到了，有provider对应的target更新了（已经成功），等于那个操作回调
				sentUpdates.WithLabelValues(m.name).Inc() // 发送一次+1
				select {
				case m.syncCh <- m.allGroups(): // 生成基于当前所有target的map，放入syncCh
				default:
					delayedUpdates.WithLabelValues(m.name).Inc() // 延迟计数+1
					level.Debug(m.logger).Log("msg", "Discovery receiver's channel was full so will retry the next cycle")
					select {
					case m.triggerSend <- struct{}{}: // 放入一个空的到triggerSend，就是这次没成功，下个间隔还得继续
					default:
					}
				}
			default: // 间隔到了，无东西需要发送
			}
		}
	}
}

func (m *Manager) cancelDiscoverers() {
	m.mtx.RLock()
	defer m.mtx.RUnlock()
	for _, p := range m.providers {
		if p.cancel != nil {
			p.cancel()
		}
	}
}

// poolkey：job+ sd类型
// tgs：变更后
func (m *Manager) updateGroup(poolKey poolKey, tgs []*targetgroup.Group) {
	m.targetsMtx.Lock()
	defer m.targetsMtx.Unlock()

	// 怎么都要有当前provider+对应job的target
	if _, ok := m.targets[poolKey]; !ok {
		m.targets[poolKey] = make(map[string]*targetgroup.Group)
	}
	// 遍历tgs，但一般只有1个，更新到manager的当前provider+对应job的target的source中
	for _, tg := range tgs {
		if tg != nil { // Some Discoverers send nil target group so need to check for it to avoid panics.
			m.targets[poolKey][tg.Source] = tg
		}
	}
}

// 返回所有job对应的，实例数组
func (m *Manager) allGroups() map[string][]*targetgroup.Group {
	// key：job，v：实例数组
	tSets := map[string][]*targetgroup.Group{}
	n := map[string]int{}

	m.targetsMtx.Lock()
	defer m.targetsMtx.Unlock()
	// 等于所有的实例按照job名聚合一起
	// 遍历所有1级target
	for pkey, tsets := range m.targets {
		// 遍历2级
		for _, tg := range tsets {
			// Even if the target group 'tg' is empty we still need to send it to the 'Scrape manager'
			// to signal that it needs to stop all scrape loops for this target set.
			// 把2级数组（详细实例）加入到tSet，key为jobname
			tSets[pkey.setName] = append(tSets[pkey.setName], tg)
			n[pkey.setName] += len(tg.Targets)
		}
	}
	for setName, v := range n {
		discoveredTargets.WithLabelValues(m.name, setName).Set(float64(v))
	}
	return tSets
}

// registerProviders returns a number of failed SD config.
// Configs：job里的服务发现数组
func (m *Manager) registerProviders(cfgs Configs, setName string) int {
	var (
		failed int
		added  bool
	)
	// 定义创建并添加provider函数
	add := func(cfg Config) {
		// 已有，就不加了
		for _, p := range m.providers {
			if reflect.DeepEqual(cfg, p.config) {
				p.newSubs[setName] = struct{}{}
				added = true
				return
			}
		}
		// 调用2个接口方法
		// 这个可以在discovery下的各种类型目录下自己的go文件中定义
		// 如eureka：https://vscode.dev/github/renrey/prometheus/blob/44fcf876caad9a5d28b92a728d2c98c34015d377/discovery/eureka/eureka.go#L120
		// 至于这些插件注册，可看Readme中的操作
		typ := cfg.Name() // sd类型名
		d, err := cfg.NewDiscoverer(DiscovererOptions{
			Logger:            log.With(m.logger, "discovery", typ),
			HTTPClientOptions: m.httpOpts,
		}) // sd注册对象
		if err != nil {
			level.Error(m.logger).Log("msg", "Cannot create service discovery", "err", err, "type", typ)
			failed++
			return
		}
		// 新增provider到manager的providers中
		// 并且加入key= setName（job）空map到newSubs
		m.providers = append(m.providers, &Provider{
			name:   fmt.Sprintf("%s/%d", typ, m.lastProvider),
			d:      d,
			config: cfg,
			newSubs: map[string]struct{}{
				setName: {},
			},
		})
		m.lastProvider++
		added = true
	}
	// 遍历sd数组，执行添加创建provider！！！
	for _, cfg := range cfgs {
		add(cfg)
	}
	if !added {
		// Add an empty target group to force the refresh of the corresponding
		// scrape pool and to notify the receiver that this target set has no
		// current targets.
		// It can happen because the combined set of SD configurations is empty
		// or because we fail to instantiate all the SD configurations.
		add(StaticConfig{{}})
	}
	return failed
}

// StaticProvider holds a list of target groups that never change.
type StaticProvider struct {
	TargetGroups []*targetgroup.Group
}

// Run implements the Worker interface.
func (sd *StaticProvider) Run(ctx context.Context, ch chan<- []*targetgroup.Group) {
	// We still have to consider that the consumer exits right away in which case
	// the context will be canceled.
	select {
	case ch <- sd.TargetGroups:
	case <-ctx.Done():
	}
	close(ch)
}
