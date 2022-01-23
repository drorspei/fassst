package utils

type Unit = struct{}

var U = Unit{}

type Set = map[string]Unit

func NewSet(input ...string) Set {
	res := make(Set, len(input))
	for _, i := range input {
		res[i] = U
	}
	return res
}
