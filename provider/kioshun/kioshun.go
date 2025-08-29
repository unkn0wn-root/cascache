package kioshun

import (
	"context"
	"time"

	pr "github.com/unkn0wn-root/cascache/provider"
	kc "github.com/unkn0wn-root/kioshun"
)

// use K=string, V=[]byte to satisfy the “byte-for-byte transparent” contract.
type Kioshun struct {
	c *kc.InMemoryCache[string, []byte]
}

var _ pr.Provider = (*Kioshun)(nil)

type Config struct {
	MaxItems               int64             // total item capacity; 0 = unlimited
	ShardCount             int               // 0 = auto (CPU * multiplier)
	Policy                 kc.EvictionPolicy // LRU/LFU/FIFO/AdmissionLFU
	CleanupInterval        time.Duration     // 0 = disable background cleanup
	AdmissionResetInterval time.Duration     // only used by AdmissionLFU
	StatsEnabled           bool
}

// - We force DefaultTTL=0 in kioshun so per-call TTL from Set() is authoritative.
// - cascache’s Set(ttl<=0) => “no expiry”; we translate that to kioshun’s NoExpiration.
func New(cfg Config) *Kioshun {
	kcfg := kc.Config{
		MaxSize:                cfg.MaxItems,
		ShardCount:             cfg.ShardCount,
		CleanupInterval:        cfg.CleanupInterval,
		DefaultTTL:             0, // provider passes TTL explicitly; 0 here means “no default”
		EvictionPolicy:         cfg.Policy,
		StatsEnabled:           cfg.StatsEnabled,
		AdmissionResetInterval: cfg.AdmissionResetInterval,
	}
	return &Kioshun{
		c: kc.New[string, []byte](kcfg),
	}
}

func NewWithCache(c *kc.InMemoryCache[string, []byte]) *Kioshun { return &Kioshun{c: c} }

func (p *Kioshun) Get(_ context.Context, key string) ([]byte, bool, error) {
	v, ok := p.c.Get(key)
	if !ok {
		return nil, false, nil
	}
	return v, true, nil
}

// kioshun’s Set has no (ok) result. We detect admission refusal by checking existence
// after Set. For new keys that were rejected, Exists() will be false.
// For updates, the key already exists and will remain true → ok=true.
//   - ttl<=0 => “no expiry” (translated to kioshun.NoExpiration)
//   - cost is ignored (kioshun is item-capacity based, not cost-based)
//   - ok==false && err==nil indicates an *intentional* refusal (AdmissionLFU under pressure)
func (p *Kioshun) Set(_ context.Context, key string, value []byte, _ int64, ttl time.Duration) (bool, error) {
	if ttl <= 0 {
		ttl = kc.NoExpiration
	}
	if err := p.c.Set(key, value, ttl); err != nil {
		return false, err
	}

	ok := p.c.Exists(key)
	return ok, nil
}

func (p *Kioshun) Del(_ context.Context, key string) error {
	_ = p.c.Delete(key)
	return nil
}

func (p *Kioshun) Close(_ context.Context) error {
	return p.c.Close()
}
