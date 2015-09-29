package driver

import (
	"encoding/json"
	"io"
	"os"
)

// PrefetchJSONRows determines the number of rows to scan in a JSON document
// to determine the available column names.
var PrefetchJSONRows = 20

type jsonDriver struct{}

func (j jsonDriver) Open(filename string) (Conn, error) {
	// open the file, read the first N records to get some "column" names
	f, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	dec := json.NewDecoder(f)

	mycols := make(map[string]struct{})

	var row map[string]interface{}
	for i := 0; i < PrefetchJSONRows; i++ {
		err = dec.Decode(&row)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		for key := range row {
			mycols[key] = struct{}{}
		}
	}

	colArray := make([]string, 0, len(mycols))
	for key := range mycols {
		colArray = append(colArray, key)
	}

	return &jsonConn{filename, colArray}, nil
}

type jsonConn struct {
	filename string
	cols     []string
}

func (j *jsonConn) Columns() []string {
	return j.cols
}

func (j *jsonConn) QueryColumns(cols []int, offset, limit uint64) (Rows, error) {
	subset := make([]string, len(cols))
	for i, x := range cols {
		subset[i] = j.cols[x]
	}

	f, err := os.Open(j.filename)
	if err != nil {
		return nil, err
	}

	row := make(map[string]string)
	dec := json.NewDecoder(f)

	// FIXME: very slow if offset is large...
	for i := uint64(0); i < offset; i++ {
		err = dec.Decode(&row)
		if err != nil {
			f.Close()
			return nil, err
		}
	}

	return &jsonRows{
		f:     f,
		dec:   dec,
		cols:  subset,
		data:  row,
		limit: limit,
	}, nil
}

func (j *jsonConn) Close() error {
	return nil
}

type jsonRows struct {
	f     *os.File
	dec   *json.Decoder
	cols  []string
	data  map[string]string
	limit uint64
}

func (j *jsonRows) Next(data []*string) error {
	// NB wrap around ok because it'll only happen
	// if limit=0 initially (i.e. caller wanted all rows)
	j.limit--
	if j.limit == 0 {
		return io.EOF
	}

	for k := range j.data {
		delete(j.data, k)
	}

	err := j.dec.Decode(&j.data)
	if err != nil {
		return err
	}

	for i, key := range j.cols {
		if v, f := j.data[key]; !f {
			data[i] = nil
		} else {
			*data[i] = v
		}
	}

	return nil
}

func (j *jsonRows) Close() error {
	return j.f.Close()
}

func init() {
	Register("json", &jsonDriver{})
}
