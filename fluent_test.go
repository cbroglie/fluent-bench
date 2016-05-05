package main

import (
	"testing"
)

func TestTCP(t *testing.T) {
	test(t, false)
}

func TestScribe(t *testing.T) {
	test(t, true)
}

func test(t *testing.T, useScribe bool) {
	f := &fluent{}
	if useScribe {
		if err := f.connectScribe(); err != nil {
			t.Fatal(err)
		}
	} else {
		if err := f.connectTCP(); err != nil {
			t.Fatal(err)
		}
	}
	defer f.close()

	m := &message{
		Tag: "tag",
		Record: map[string]interface{}{
			"key": "blah",
		},
	}
	err := f.sendMessage(m)
	if err != nil {
		t.Fatal(err)
	}
}

func BenchmarkLocalFile(b *testing.B) {
	/*
		<match local.**>
		  type file
		  path /var/log/td-agent/access
		</match>
	*/
	benchmark("local.*", b)
}

func BenchmarkStdout(b *testing.B) {
	/*
		<match debug.**>
		  type stdout
		</match>
	*/
	benchmark("debug.*", b)
}

func BenchmarkNoMatch(b *testing.B) {
	benchmark("nomatch", b)
}

func benchmark(tag string, b *testing.B) {
	f := &fluent{}
	if err := f.connectTCP(); err != nil {
		b.Fatal(err)
	}
	defer f.close()

	m := &message{
		Tag: tag,
		Record: map[string]interface{}{
			"key": "blah",
		},
	}
	for i := 0; i < b.N; i++ {
		err := f.sendMessage(m)
		if err != nil {
			b.Fatal(err)
		}
	}
}
