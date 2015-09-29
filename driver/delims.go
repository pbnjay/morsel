package driver

import (
	"bufio"
	"fmt"
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

func (d *delimConn) QueryColumns(cols []int, offset, limit int) (Rows, error) {
	if offset != 0 || limit != 0 {
		return nil, fmt.Errorf("morsel-json: offset/limit not yet implemented")
	}

	f, err := os.Open(d.filename)
	if err != nil {
		return nil, err
	}
	s := bufio.NewScanner(f)
	return &delimRows{
		d:    d.d,
		f:    f,
		s:    s,
		cols: cols,
		row:  make([]string, len(d.cols)),
	}, nil
}

func (d *delimConn) Close() error {
	return nil
}

type delimRows struct {
	d    *delimDriver
	f    *os.File
	s    *bufio.Scanner
	cols []int
	row  []string
}

func (d *delimRows) Next(data []*string) error {
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
