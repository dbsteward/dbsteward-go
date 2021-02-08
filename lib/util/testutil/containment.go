package testutil

import (
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func AssertContainsSubseq(t *testing.T, list, subseq interface{}) (bool, int, int) {
	match, start, end := containsSubseq(list, subseq)
	if !match {
		assert.Fail(t,
			fmt.Sprintf("List does not contain subsequence:\n  list: %s\n  subseq: %s",
				pplistAnnotate(list, "  ", start, end),
				pplistAnnotate(subseq, "  ", 0, end-start)),
		)
	}
	return match, start, end
}

func AssertNotContainsSubseq(t *testing.T, list, subseq interface{}) (bool, int, int) {
	match, start, end := containsSubseq(list, subseq)
	if match {
		assert.Fail(t,
			fmt.Sprintf("List contains subsequence:\n  list: %s\n  subseq: %s",
				pplistAnnotate(list, "  ", start, end),
				pplistAnnotate(subseq, "  ", 0, end-start)),
		)
	}
	return !match, start, end
}

// returns (match, start, end)
// where start is the first index of the matched subsequence in list and end is the last matching index + 1
// if match == false and start == 0 and end == 0, no partials were matched
// TODO(go,nth) this is "greedy", so containsSubseq([1 2 3 2 3 4], [2 3 4]) will fail
// instead of matching the second (2 3 4) sub-sequence. we should fix that if/when necessary
func containsSubseq(list, subseq interface{}) (bool, int, int) {
	// TODO(go,nth) check types/kinds of list/subseq, make sure they're slices
	listVal := reflect.ValueOf(list)
	subseqVal := reflect.ValueOf(subseq)

	// all lists contain empty subsequences
	if subseqVal.Len() == 0 {
		return true, 0, 0
	}
	// the empty list contains no subsequences (except for the empty subsequence)
	if listVal.Len() == 0 {
		return false, 0, 0
	}

	// for each element `i` in the `list`, check if it's equal to the current element `j`
	// in `subseq`. Once we find a match, all `i` must equal `j`, and `j` is incremented.
	// once we start matching (`j > 0`) then a non-match fails immediately. We can also bail
	// early as soon as j >= len(subseq) because we've successfully matched the whole subset
	i := 0
	j := 0
	start := -1
	for ; i < listVal.Len() && j < subseqVal.Len(); i += 1 {
		elem := listVal.Index(i).Interface()
		ssElem := subseqVal.Index(j).Interface()
		if assert.ObjectsAreEqual(elem, ssElem) {
			if start == -1 {
				start = i
			}
			j += 1
		} else if j > 0 {
			if start < 0 {
				start = 0
			}
			return false, start, i
		}
	}
	// if j < len(subseq), we haven't actually matched the whole subseq yet, so that's a failure
	end := i
	if start < 0 {
		start = 0
		end = 0
	}
	return j >= subseqVal.Len(), start, end
}

func pplistAnnotate(list interface{}, indent string, start, end int) string {
	listVal := reflect.ValueOf(list)
	if listVal.Len() == 0 {
		return "[]"
	}

	horiz := "["
	vert := "["
	for i := 0; i < listVal.Len(); i += 1 {
		item := fmt.Sprintf("%v", ppval(listVal.Index(i)))
		if start >= 0 && i >= start && i < end {
			// if we're in the annotation range
			// surround horiz range with (), prefix vert with >
			if i == start {
				horiz += "("
			}
			horiz += item
			if i == end-1 {
				horiz += ")"
			}
			horiz += " "

			vert += fmt.Sprintf("\n%s> %v", indent, item)
		} else {
			horiz += fmt.Sprintf("%v ", item)
			vert += fmt.Sprintf("\n%s  %v", indent, item)
		}
	}
	horiz = strings.TrimSpace(horiz) + "]"
	vert += "\n" + indent + "]"
	if len(horiz) <= 120 {
		return horiz
	}
	return vert
}

func ppval(val reflect.Value) string {
	if val.Kind() == reflect.Ptr || val.Kind() == reflect.Interface {
		return ppval(val.Elem())
	}
	return fmt.Sprintf("%s%v", val.Type(), val)
}
