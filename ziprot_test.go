package ziprot

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"
)

func TestSmallFiles(t *testing.T) {
	z, err := New("TestSmallFiles")
	if err != nil {
		t.Fatalf("%v", err)
	}
	str := "0123456789"
	z.MaxFiles(5).MaxSize(32)
	for i := 0; i < 100; i++ {
		if _, err = fmt.Fprint(z, str); err != nil {
			t.Fatalf("%v", err)
		}
	}
	cmp := ""
	for i := 0; i < 20; i++ {
		cmp += str
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
	contents := map[string]string{}
	for _, file := range files {
		if file == "TestSmallFiles" {
			baseFound = true
		}
		if strings.Index(file, "TestSmallFiles") == 0 {
			count++
			f, err := os.Open(file)
			if err != nil {
				t.Fatalf("%v", err)
			}
			buf := &bytes.Buffer{}
			if _, err = io.Copy(buf, f); err != nil {
				t.Fatalf("%v", err)
			}
			if err = os.Remove(file); err != nil {
				t.Fatalf("%v", err)
			}
			contents[file] = buf.String()
			if err = f.Close(); err != nil {
				t.Fatalf("%v", err)
			}
		}
	}
	if err = dir.Close(); err != nil {
		t.Fatalf("%v", err)
	}
	if !baseFound {
		t.Errorf("Couldn't find the base file")
	}
	if count != 6 {
		t.Errorf("Not enough files found")
	}
	sum := ""
	sum += contents["TestSmallFiles"]
	for i := 1; i < 6; i++ {
		sum += contents[fmt.Sprintf("TestSmallFiles.%v", i)]
	}
	if sum != cmp {
		t.Errorf("Got %#v (len %v) but wanted %#v (len %v)", sum, len(sum), cmp, len(cmp))
	}
}
