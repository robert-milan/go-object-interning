package goi

import (
	"testing"
)

func TestCompressionDecompress(t *testing.T) {
	oi := NewObjectIntern(nil)
	testBytes := [][]byte{
		[]byte("SmallString"),
		[]byte("LongerString"),
		[]byte("AnEvenLongerString"),
		[]byte("metric"),
		[]byte("AndTheLongestStringWeDealWithWithEvenASmallAmountOfSpaceMoreToGetUsOverTheGiganticLimitOfStuff"),
	}
	testResults := make([][]byte, 0)

	for _, b := range testBytes {
		c := oi.Compress(b)
		d, err := oi.Decompress(c)
		if err != nil {
			continue
		}
		testResults = append(testResults, d)
	}

	for i, res := range testResults {
		for k, v := range res {
			if v != testBytes[i][k] {
				t.Error("Mismatched")
				return
			}
		}
	}
}
