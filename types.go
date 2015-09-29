// Package morsel provides a database/sql -like interface for working with
// simple file-based data sources such as json and delimited text.
package morsel

import (
	"fmt"

	"github.com/pbnjay/morsel/driver"
)

// A Morsel is an interface to a document consisting of Rows and Columns.
type Morsel struct {
	driver driver.Conn
}

// QueryColumns enumerates Rows in a Morsel by their index (e.g. in delimited
// text, the document order from left to right).
func (m *Morsel) QueryColumns(cols []int) (*Rows, error) {
	r, err := m.driver.QueryColumns(cols, 0, 0)
	return &Rows{r, make([]*string, len(cols)), nil}, err
}

// QueryNames enumerates Rows in a Morsel by their name (e.g. in delimited
// text, the names found in the first line header, or by json keys).
func (m *Morsel) QueryNames(colNames []string) (*Rows, error) {
	colInts := make([]int, len(colNames))
	cols := m.driver.Columns()
	for j, k2 := range colNames {
		colInts[j] = -1

		for i, k1 := range cols {
			if k2 == k1 {
				colInts[j] = i
				break
			}
		}

		if colInts[j] == -1 {
			return nil, fmt.Errorf("morsel: column '%s' not found", k2)
		}
	}
	r, err := m.driver.QueryColumns(colInts, 0, 0)
	return &Rows{r, make([]*string, len(cols)), nil}, err
}

// Rows provides a way to read individual data from each row enumerated.
type Rows struct {
	r   driver.Rows
	row []*string
	Err error
}

// Next fetches the next(first) result from the data source, and returns
// true if one was available. If no more data is available (or an error
// occurs), it returns false.
func (r *Rows) Next() bool {
	r.Err = r.r.Next(r.row)
	return r.Err == nil
}

// Scan copies the resulting data into the pointer arguments specified,
// according to the same order provided to Morsel.Query*() methods
func (r *Rows) Scan(args ...*string) (err error) {
	defer func() {
		if x := recover(); x != nil {
			err = fmt.Errorf("morsel: panic during scan - %s", x)
		}
	}()

	for i, v := range r.row {
		if v == nil {
			args[i] = v
		} else {
			*args[i] = *v
		}
	}
	return nil
}

// Close releases any associated memory and closes file pointers
func (r *Rows) Close() error {
	return r.r.Close()
}

// Open finds the appropriate driver by name, and opens the source file, and
// returns a Morsel instance that can be used to enumerate data.
func Open(driverName, source string) (*Morsel, error) {
	dc, err := driver.Open(driverName, source)
	if err != nil {
		return nil, err
	}

	return &Morsel{dc}, nil
}
