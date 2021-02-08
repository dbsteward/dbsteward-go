package testutil_test

import (
	"testing"

	"github.com/dbsteward/dbsteward/lib/util/testutil"
	"github.com/stretchr/testify/assert"
)

func TestContainsSubseq(t *testing.T) {
	list := []int{1, 2, 3, 4, 5, 6, 7}

	cases := []struct {
		list   []int
		subseq []int
		match  bool
		start  int
		end    int
	}{
		// 0: the subsequence can appear at the start of the list
		{list, []int{1, 2, 3}, true, 0, 3},
		// 1: or the middle of the list
		{list, []int{3, 4, 5}, true, 2, 5},
		// 2: or the end of the list
		{list, []int{5, 6, 7}, true, 4, 7},
		// 3: the empty sequence is always present
		{list, []int{}, true, 0, 0},
		// 4: even in empty lists
		{[]int{}, []int{}, true, 0, 0},
		// 5: and the whole list is a subsequence of itself
		{list, list, true, 0, 7},

		// 6: a value that never appears in the list can never be a subsequence
		{list, []int{0}, false, 0, 0},
		// 7: even if some of the elements are present
		{list, []int{0, 1, 2}, false, 0, 0},
		// 8:
		{list, []int{1, 2, 7}, false, 0, 2},
		// 9: elements must be subsequent, not merely present and in order
		{list, []int{1, 3, 5}, false, 0, 1},
		// 10:
		{list, []int{4, 3, 2}, false, 3, 4},
		// 11: if the subseq extends beyond the list, we have not matched
		{list, []int{6, 7, 8}, false, 5, 7},
		// 12: the empty list contains no subsequences
		{[]int{}, []int{1, 2}, false, 0, 0},
	}

	for i, c := range cases {
		var match bool
		var start, end int
		if c.match {
			match, start, end = testutil.AssertContainsSubseq(t, c.list, c.subseq)
		} else {
			match, start, end = testutil.AssertNotContainsSubseq(t, c.list, c.subseq)
		}
		assert.True(t, match, "case %d expected assertion to pass", i)
		assert.Equal(t, c.start, start, "case %d unexpected subseq start", i)
		assert.Equal(t, c.end, end, "case %d unexpected subseq end", i)
	}
}
