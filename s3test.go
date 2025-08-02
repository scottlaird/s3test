package main

// This is an demonstration of a problem with S3 reads from SeaweedFS
// 3.96 and below, *not* an example of how to properly read from S3.
//
// Specifically, it mimics the read patterns that SeaweedFS sees when
// streaming video over HTTP via Caddy.  In this case, the browser
// issues range requests asking for a few seconds of video from the
// middle of a (potentially at least) multi-gigabyte file.
//
// Since each HTTP request is independent and only asks for maybe 1%
// of the file, we *really* don't want to read additional bytes
// anywhere in the pipeline.  If the web server wants 1 MB from a 1 GB
// file, then it should read 1 MB, the filer should read 1 MB, and the
// volume server should read 1 MB.
//
// Unfortunately, that's not what is happening for me.  When reading 1
// MB at a time from SeaweedFS, I'm seeing around 100 Mbps received in
// this code and 40+ Gbps of extra network traffic between volume
// servers and filers.
//
// Example:
// - filer+volume A:       tx 30 Gbps    rx 15 Gbps
// - filer+volume B:       tx 15 Gbps    rx 30 Gbps
// - client/test machine   tx 0.5 Mbps   rx 35 Mbps
//
// So this isn't a case of the Go S3 library doing extra reads on the
// client side.  All of the extra traffic seems to be between filers
// and volume servers.
//
// In my specific case, I have 2 machines running filers (w/ S3
// in-process).  They each also run volume servers, plus there are 8
// additional volume servers.  Then there are 3 masters.  All are
// currently running 3.96.

// To reproduce, you'll need a SeaweedFS instance with S3 running and
// a 100+ MB file in a bucket that's accessible via the S3 API.
//
// To build, just run:
//
// $ go build
//
// To test, you'll need to provide an endpoint and a bucket name:
//
// $ ./s3test --endpoint http://s3:8333 --bucket webvideo my/file/name.mp4
//
// You can vary the read size with --readsize, the default is 256 kB.
// To use a 1 MB read size, use `--readsize 1048576`, etc.
//
// Ideally, watch the network load on volume server(s) and filer(s)
// while running this.  Alternately, watch the S3 latency or the
// number of filer threads; they should both skyrocket up *and remain
// high for a little while after this test completes*.  Or at least
// they will if you load them up high enough.  Small reads, big file
// -> overloaded SeaweedFS.

// SeaweedFS acts like it's reading the entire file in the filer
// instead of just the 1 MB segment that I've requested.  During these
// runs (or at least the slow ones) I'm seeing 90th percentile S3
// latencies (via Prometheus and Grafana) of over 1 minute, vs a filer
// 90th percentile latency reported at ~5 ms.

// Sample results:

// Timing from reading a small (40 MB) sample file with different
// --readsize flags:
//
// 64 kB reads: Read 40304640 bytes in 4.728 seconds at 68.200492 Mbps
// 256 kB reads: Read 40108032 bytes in 1.229 seconds at 260.987696 Mbps
// 16 MB reads: Read 33554432 bytes in 0.082 seconds at 3289.776143 Mbps
//
// (Note that this code doesn't attempt to do a partial read for the
// final chunk, so the actual total number of bytes vary.  Pay
// attention to the Mbps number for comparison.)

// Increasing to a slightly larger file (143 MB):
//
// 64 kB reads: Read 143654912 bytes in 38.588 seconds at 29.782115 Mbps
// 256 kb reads: Read 143654912 bytes in 9.521 seconds at 120.701943 Mbps
// 16 MB reads: Read 134217728 bytes in 0.339 seconds at 3165.464318 Mbps
//
// Note that the Mbps numbers on the smaller read sizes fell
// substantially when compared to the smaller file, while 16 MB reads
// stayed basically flat.

// Now, if I increase to a 600 MB file:
//
// 64 kB reads: too slow to measure, probably hours.
// 256 kB reads: 33-800 ms per read.  Quit after several minutes.
// 1 MB reads: Read 909115392 bytes in 168.789 seconds at 43.088844 Mbps
// 16 MB reads: Read 905969664 bytes in 4.814 seconds at 1505.469302 Mbps
// 256 MB reads: Read 805306368 bytes in 1.587 seconds at 4060.120409 Mbps
//
// Even 16 MB reads started to slow down here, while 256 MB reads were
// fairly fast.

// Note that the actual network `Read()` requests are mostly only
// returning ~32k each, so I'm reading in a loop to make sure that I
// read the total number of bytes requested.  So it's not like the
// actual network traffic for 16M reads is that different from 64k
// reads -- they both consist of a bunch of smallish `Read()` calls.

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/jszwec/s3fs/v2"
)

var (
	endpoint = flag.String("endpoint", "http://s3.internal.sigkill.org:8333", "endpoint for talking to SeaweedFS's S3 interface")
	bucket   = flag.String("bucket", "webvideo", "s3 bucket to read from")
	region   = flag.String("region", "none", "s3 region to read from")
	readsize = flag.Int("readsize", 1<<18, "number of bytes to read per file open")
)

// Set up the Go s3fs client, as used by Caddy.
func connect(ctx context.Context) (*s3fs.S3FS, error) {
	config, err := config.LoadDefaultConfig(
		ctx,
		config.WithRegion(*region),
	)
	if err != nil {
		return nil, err
	}

	client := s3.NewFromConfig(config, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(*endpoint)
		o.UsePathStyle = true
		o.DisableLogOutputChecksumValidationSkipped = true
	})
	fs := s3fs.New(client, *bucket, s3fs.WithReadSeeker)

	return fs, nil
}

// Read `size` bytes at `offset` from `filename` via `fs`.
func readFrom(fs *s3fs.S3FS, filename string, offset uint64, size uint64, totalsize uint64) error {
	start := time.Now()

	f, err := fs.Open(filename)
	defer f.Close()
	if err != nil {
		return err
	}

	// fs.Open() returns a fs.FS, which is an interface that
	// doesn't include `Seek`, although the underlying
	// implementation does support it.  Casting it to
	// `io.ReadSeeker` is the recommended way to fix this.
	fSeek := f.(io.ReadSeeker)

	_, err = fSeek.Seek(int64(offset), 0)
	if err != nil {
		return err
	}

	b := make([]byte, size)

	var curOffset uint64
	var n int

	for {
		n, err = f.Read(b[curOffset:])
		if err != nil {
			return err
		}
		curOffset += uint64(n)
		if curOffset >= size {
			break
		}
	}

	dur := time.Since(start)

	fmt.Printf("Read %d bytes at offset %d in %.3fs (%.1f%%)\n", curOffset, offset, dur.Seconds(), float64(100*offset)/float64(totalsize))

	return nil
}

func main() {
	flag.Parse()

	filename := flag.Arg(0)
	if len(filename) == 0 {
		fmt.Printf("Please provide a filename, and optionally --endpoint= and --bucket= args\n")
		os.Exit(1)
	}

	ctx := context.Background()
	fs, err := connect(ctx)
	if err != nil {
		panic(err)
	}

	// Figure out how big the file is
	fileinfo, err := fs.Stat(filename)
	if err != nil {
		panic(err)
	}
	filesize := uint64(fileinfo.Size())

	readSize := uint64(*readsize)
	readCount := uint64(filesize) / readSize // this leaves off the end of the file, which is fine for this use.

	var i uint64

	start := time.Now()

	// Read from the file repeatedly, pretending that we're a HTTP server feeding video to a client.
	for i = 0; i < readCount; i++ {
		offset := readSize*i
		err = readFrom(fs, filename, offset, readSize, readSize*readCount)
		if err != nil {
			panic(err)
		}
	}
	dur := time.Since(start)
	fmt.Printf("Read %d bytes in %.3f seconds at %f Mbps\n", readSize*readCount, dur.Seconds(), float64(readSize*readCount*8)/dur.Seconds()/1000000)
}
