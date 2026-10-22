package cascache

// SetOnJoin sets a hook called when a Load joins a run in progress.
func (c *Cache[V]) SetOnJoin(f func(key string)) { c.fills.OnJoin = f }
