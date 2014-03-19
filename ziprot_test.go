package ziprot

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"
)

func validateFiles(t *testing.T, prefix string, cmp string, wantedCount int) {
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
		if file == prefix {
			baseFound = true
		}
		if strings.Index(file, prefix) == 0 {
			count++
			f, err := os.Open(file)
			if err != nil {
				t.Fatalf("%v", err)
			}
			reader, err := gzip.NewReader(f)
			if err == nil {
				buf := &bytes.Buffer{}
				io.Copy(buf, reader)
				contents[file] = buf.String()
				reader.Close()
			}
			if err = os.Remove(file); err != nil {
				t.Fatalf("%v", err)
			}
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
	if count != wantedCount {
		t.Errorf("Not enough files found")
	}
	sum := ""
	sum += contents["TestSmallFiles"]
	for i := 1; i < count; i++ {
		sum += contents[fmt.Sprintf("%v.%v", prefix, i)]
	}
	if sum != cmp {
		t.Errorf("Got %#v (len %v) but wanted %#v (len %v)", sum, len(sum), cmp, len(cmp))
	}
}

func TestMediumFiles(t *testing.T) {
	z, err := New("TestMediumFiles")
	if err != nil {
		t.Fatalf("%v", err)
	}
	str := "0123456789"
	z.MaxFiles(5).MaxSize(1024)
	for i := 0; i < 100000; i++ {
		if _, err = fmt.Fprint(z, str); err != nil {
			t.Fatalf("%v", err)
		}
	}
	cmp := ""
	for i := 0; i < 630; i++ {
		cmp += str
	}
	validateFiles(t, "TestMediumFiles", cmp, 6)
}

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
	for i := 0; i < 10; i++ {
		cmp += str
	}
	validateFiles(t, "TestSmallFiles", cmp, 6)
}
