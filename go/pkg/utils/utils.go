package utils

type Unit = struct{}

var U = Unit{}

func Min(x, y uint64) uint64 {
	if x < y {
		return x
	}
	return y
}
