package ziprot

import (
	"fmt"
	"os"
	"strings"
	"testing"
)

func TestSmallFiles(t *testing.T) {
	z, err := New("TestSmallFiles")
	if err != nil {
		return
	}
	z.MaxFiles(5).MaxSize(32)
	for i := 0; i < 100; i++ {
		if _, err = fmt.Fprint(z, "0123456789"); err != nil {
			t.Fatalf("%v", err)
		}
	}
	dir, err := os.Open(".")
	if err != nil {
		t.Fatalf("%v", err)
	}
	files, err := dir.Readdirnames(-1)
	if err != nil {
		t.Fatalf("%v", err)
	}
	count := 0
	baseFound := false
	for _, file := range files {
		if file == "TestSmallFiles" {
			baseFound = true
		}
		if strings.Index(file, "TestSmallFiles") == 0 {
			count++
			if err = os.Remove(file); err != nil {
				t.Fatalf("%v", err)
			}
		}
	}
	if !baseFound {
		t.Errorf("Couldn't find the base file")
	}
	if count != 6 {
		t.Errorf("Not enough files found")
	}
}
