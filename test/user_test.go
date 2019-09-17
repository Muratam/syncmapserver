// go test ./test
package syncmapserver

import "testing"

var x MutexInt

func TestSum(t *testing.T) {
	a, b, want := 1, 2, 3
	if got := a + b; got != want {
		t.Fatalf("want = %d, got = %d", want, got)
	}
}
