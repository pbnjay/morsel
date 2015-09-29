// Command eat opens a morsel document and enumerates it.
package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/pbnjay/morsel"
)

var (
	numeric    = flag.Bool("n", false, "columns are specified numerically instead of by name")
	morselType = flag.String("t", "tsv", "morsel document format type")
)

func main() {
	flag.Parse()

	m, err := morsel.Open(*morselType, flag.Arg(0))
	if err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: %s", err)
		os.Exit(1)
	}

	start := time.Now()
	args := flag.Args()[1:]
	oneRow := make([]*string, len(args))
	var rows *morsel.Rows
	if *numeric {
		cols := make([]int, len(args))
		for i, s := range args {
			_, err = fmt.Sscanf(s, "%d", &cols[i])
			if err != nil {
				fmt.Fprintf(os.Stderr, "error parsing column integer '%s'", s)
				os.Exit(1)
			}
		}

		rows, err = m.QueryColumns(cols)
	} else {

		rows, err = m.QueryNames(args)
	}
	for i := range args {
		x := ""
		oneRow[i] = &x
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "error executing query: ", err)
		os.Exit(1)
	}
	defer rows.Close()

	n := 0
	for rows.Next() {
		err = rows.Scan(oneRow...)
		if err != nil {
			fmt.Fprintln(os.Stderr, "error reading rows: ", err)
			os.Exit(1)
		}
		for i, c := range oneRow {
			if i > 0 {
				os.Stdout.WriteString("\t")
			}
			fmt.Fprintf(os.Stdout, "'%s'", *c)
		}
		os.Stdout.WriteString("\n")
		n++
	}
	elap := time.Now().Sub(start)
	fmt.Fprintf(os.Stderr, "%d rows in %s\n", n, elap)
}
