package cascache

// coalesce returns def when v is the zero value of T - otherwise v.
func coalesce[T comparable](v, def T) T {
	var zero T
	if v == zero {
		return def
	}
	return v
}
