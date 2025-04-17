package scanner

import (
	"io"
	"math"

	"github.com/icza/backscanner"
)

func ScanReverse(src io.ReaderAt, parseF ParseFuncT, scanF ScanFuncT, opts ...ScanOptT) error {

	var (
		err   error
		o     = parseOpts(opts)
		bufSz = (o.maxSz/pageSize + 1) * pageSize
		bopts = backscanner.Options{
			ChunkSize:     bufSz,
			MaxBufferSize: calcMaxSize(bufSz),
		}
		scanner = backscanner.NewOptions(src, int(o.mark), &bopts)
	)

	stop := o.stop
	if stop == math.MaxInt64 {
		stop = 0
	}

LOOP:
	for {
		var line []byte
		line, _, err = scanner.LineBytes()

		switch err {
		case nil:
		case io.EOF:
			err = nil
			break LOOP
		default:
			break LOOP
		}

		entry, parseErr := parseF(line)
		if parseErr != nil {
			if err := o.errF(line, parseErr); err != nil {
				return err
			}
			continue
		}

		if entry.Timestamp < stop {
			break LOOP
		}

		if scanF(entry) {
			break LOOP
		}
	}

	return err
}

func calcMaxSize(chunkSz int) int {
	maxSize := 1024 * 1024
	if maxSize < chunkSz {
		maxSize = chunkSz * 2
	}
	return maxSize
}
