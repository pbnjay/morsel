// Package driver defines file-type-specific drivers that can be used with
// morsel.
package driver

import "fmt"

// Driver defines a Morsel resource that can manipulate a text-based
// record format.
type Driver interface {
	Open(filename string) (Conn, error)
}

// Conn defines the methods for interacting with a Morsel data source.
type Conn interface {
	// Columns returns the header/column names for the data source. Blank
	// names are allowed when none are available, and if the data source
	// has NO column information available, an empty slice is acceptable.
	Columns() []string

	// QueryColumns begins an enumeration from the beginning offset for
	// limit number of rows. An offset=0 and limit=0 indicate all data.
	QueryColumns(cols []int, offset, limit int) (Rows, error)

	// Close closes the conn and releases any resources including
	// outstanding Rows instances.
	Close() error
}

// Rows describes a result from Conn.QueryColumns over a potential subset
// of records. Rows know the subset of columns they are using, and only
// return those entries.
type Rows interface {
	// Next advances to the next record, and copies the column data into
	// the provided slice. Pointers to nil are used to indicate missing
	// versus blank results.
	Next(data []*string) error

	// Close stops enumeration and releases any resources.
	Close() error
}

///////////

var drivers = make(map[string]Driver)

// Register a new morsel driver.Driver for use by other packages.
func Register(name string, drv Driver) error {
	if _, f := drivers[name]; f {
		return fmt.Errorf("morsel: driver '%s' already registered", name)
	}
	drivers[name] = drv
	return nil
}

// Open looks up a morsel driver by name and attempts to open the named source.
func Open(driverName, source string) (Conn, error) {
	drv, found := drivers[driverName]
	if !found {
		return nil, fmt.Errorf("morsel: driver '%s' not found. (did you import it?)", driverName)
	}

	return drv.Open(source)
}
