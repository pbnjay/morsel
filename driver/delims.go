package driver

import (
	"bufio"
	"io"
	"os"
	"strings"
)

type delimDriver struct {
	delim string
}

func (d delimDriver) Open(filename string) (Conn, error) {
	f, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	// read one line, split on delim, then return
	s := bufio.NewScanner(f)
	cols := strings.Split(s.Text(), d.delim)

	return &delimConn{&d, filename, cols}, nil
}

type delimConn struct {
	d        *delimDriver
	filename string
	cols     []string
}

func (d *delimConn) Columns() []string {
	return d.cols
}

func (d *delimConn) QueryColumns(cols []int, offset, limit uint64) (Rows, error) {
	f, err := os.Open(d.filename)
	if err != nil {
		return nil, err
	}
	s := bufio.NewScanner(f)

	// FIXME: very slow if offset is large...
	for i := uint64(0); i < offset; i++ {
		s.Text()
		err = s.Err()
		if err != nil {
			f.Close()
			return nil, err
		}
	}

	return &delimRows{
		d:     d.d,
		f:     f,
		s:     s,
		cols:  cols,
		row:   make([]string, len(d.cols)),
		limit: limit,
	}, nil
}

func (d *delimConn) Close() error {
	return nil
}

type delimRows struct {
	d     *delimDriver
	f     *os.File
	s     *bufio.Scanner
	cols  []int
	row   []string
	limit uint64
}

func (d *delimRows) Next(data []*string) error {
	// NB wrap around ok because it'll only happen
	// if limit=0 initially (i.e. caller wanted all rows)
	d.limit--
	if d.limit == 0 {
		return io.EOF
	}

	d.row = d.row[:0]
	d.row = strings.Split(d.s.Text(), d.d.delim)

	for i, k := range d.cols {
		*data[i] = d.row[k]
	}

	return nil
}

func (d *delimRows) Close() error {
	return d.f.Close()
}

func init() {
	Register("tsv", &delimDriver{"\t"})
	Register("csv", &delimDriver{","})
}
