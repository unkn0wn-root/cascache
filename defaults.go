package cascache

func coalesce[T comparable](v, def T) T {
	var zero T
	if v == zero {
		return def
	}
	return v
}
