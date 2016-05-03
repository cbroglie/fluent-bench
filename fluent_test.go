package main

import (
	"testing"
)

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
	if err := f.connect(); err != nil {
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
