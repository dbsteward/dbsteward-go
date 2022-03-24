package util

import (
	"math"
	"strconv"

	"golang.org/x/exp/constraints"
)

type Number interface {
	constraints.Integer | constraints.Float
}

func Min[T constraints.Ordered](a, b T) T {
	if a < b {
		return a
	}
	return b
}

func IntCeil[T Number](num, div T) int {
	return int(math.Ceil(float64(num) / float64(div)))
}

func IntFloor[T Number](num, div T) int {
	return int(float64(num) / float64(div))
}

func MustParseInt(val string) int {
	v, err := strconv.Atoi(val)
	if err != nil {
		panic(err)
	}
	return v
}

func NumDigits(x int) int {
	return int(math.Log10(float64(x)) + 1)
}
