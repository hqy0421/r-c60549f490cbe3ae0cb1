package trdsql

import (
	"io"
	"log"
	"sync"
)

// extToFormat is a map of file extensions to formats.
var extToFormat map[string]Format = map[string]Format{
	"CSV":    CSV,
	"LTSV":   LTSV,
	"JSON":   JSON,
	"JSONL":  JSON,
	"YAML":   YAML,
	"YML":    YAML,
	"TBLN":   TBLN,
	"TSV":    TSV,
	"PSV":    PSV,
	"WIDTH":  WIDTH,
	"TEXT":   TEXT,
	"LOGFMT": LOGFMT,
}

// ReaderFunc is a function that creates a new Reader.
type ReaderFunc func(io.Reader, *ReadOpts) (Reader, error)

// readerFuncs maps formats to their corresponding ReaderFunc.
var readerFuncs = map[Format]ReaderFunc{
	CSV: func(reader io.Reader, opts *ReadOpts) (Reader, error) {
		return NewCSVReader(reader, opts)
	},
	LTSV: func(reader io.Reader, opts *ReadOpts) (Reader, error) {
		return NewLTSVReader(reader, opts)
	},
	JSON: func(reader io.Reader, opts *ReadOpts) (Reader, error) {
		return NewJSONReader(reader, opts)
	},
	YAML: func(reader io.Reader, opts *ReadOpts) (Reader, error) {
		return NewYAMLReader(reader, opts)
	},
	TBLN: func(reader io.Reader, opts *ReadOpts) (Reader, error) {
		return NewTBLNReader(reader, opts)
	},
	TSV: func(reader io.Reader, opts *ReadOpts) (Reader, error) {
		return NewTSVReader(reader, opts)
	},
	PSV: func(reader io.Reader, opts *ReadOpts) (Reader, error) {
		return NewPSVReader(reader, opts)
	},
	WIDTH: func(reader io.Reader, opts *ReadOpts) (Reader, error) {
		return NewGWReader(reader, opts)
	},
	TEXT: func(reader io.Reader, opts *ReadOpts) (Reader, error) {
		return NewTextReader(reader, opts)
	},
	LOGFMT: func(reader io.Reader, opts *ReadOpts) (Reader, error) {
		return NewLogfmtReader(reader, opts)
	},
}

var (
	// extFormat is the next format number to be assigned.
	extFormat Format = 100
	// registerMux is a mutex to protect access to the register.
	registerMux = &sync.Mutex{}
)

func RegisterReaderFunc(ext string, readerFunc ReaderFunc) {
	registerMux.Lock()
	defer registerMux.Unlock()
	extToFormat[ext] = extFormat
	readerFuncs[extFormat] = readerFunc
	extFormat++
}

// Reader wraps a tabular file reader.
type Reader interface {
	// Names returns column names.
	Names() ([]string, error)
	// Types returns column types.
	Types() ([]string, error)
	// PreReadRow returns pre-read rows.
	PreReadRow() [][]any
	// ReadRow reads the next row.
	ReadRow(row []any) ([]any, error)
}

// ReadOpts represents options that determine the behavior of the reader.
type ReadOpts struct {
	// InDelimiter is the field delimiter.
	// default is ','
	InDelimiter string

	// InNULL is a string to replace with NULL.
	InNULL string

	// InJQuery is a jq expression.
	InJQuery string

	// InFormat is the input format.
	// The supported format is CSV/LTSV/JSON/TBLN.
	InFormat   Format
	realFormat Format

	// InPreRead is the number of rows to read ahead.
	// CSV/LTSV reads the specified number of rows to
	// determine the number of columns.
	InPreRead int

	// InSkip is the number of rows to skip.
	// Skip reading specified number of lines.
	InSkip int

	// InLimitRead limits reading to pre-read rows.
	InLimitRead bool

	// InHeader is true if there is a header.
	// It is used as a column name.
	InHeader bool
	// InNeedNULL is true, replace InNULL with NULL.
	InNeedNULL bool

	// IsTemporary indicates whether to create a temporary table.
	// default is true.
	IsTemporary bool

	// InRowNumber indicates whether to add a row number column.
	InRowNumber bool

	// InBatchTable is the target table name for batch import.
	// If empty, the table name is derived from the file name.
	// It is used only when InBatchID is set.
	InBatchTable string

	// InBatchID is the batch identity of the delivered file.
	// When set, the import becomes batch aware: the delivered
	// content is fingerprinted and recorded in a batch registry,
	// and every imported row is tagged with this identity in the
	// InBatchColumn column. An exact duplicate delivery (same
	// identity and same content) is not written again. A revised
	// delivery (same identity, different content) is handled
	// according to InBatchMode.
	// If empty, batch import is disabled (default).
	InBatchID string

	// InBatchColumn is the name of the column that stores
	// the batch identity on each imported row.
	// default is "_batch_id".
	InBatchColumn string

	// InBatchMode determines how a revised batch (same batch
	// identity with different content) is handled.
	// default is BatchReplace.
	InBatchMode BatchMode
}

// NewReadOpts returns a ReadOpts.
func NewReadOpts(options ...ReadOpt) *ReadOpts {
	readOpts := &ReadOpts{
		InFormat:      GUESS,
		InPreRead:     1,
		InLimitRead:   false,
		InSkip:        0,
		InDelimiter:   ",",
		InHeader:      false,
		IsTemporary:   true,
		InJQuery:      "",
		InNeedNULL:    false,
		InNULL:        "",
		InBatchColumn: DefaultBatchColumn,
		InBatchMode:   BatchReplace,
	}
	for _, option := range options {
		option(readOpts)
	}
	return readOpts
}

// ReadOpt is a functional option for ReadOpts.
type ReadOpt func(*ReadOpts)

// InFormat sets the input format.
func InFormat(f Format) ReadOpt {
	return func(args *ReadOpts) {
		args.InFormat = f
	}
}

// InPreRead sets the number of rows to read ahead.
func InPreRead(p int) ReadOpt {
	return func(args *ReadOpts) {
		args.InPreRead = p
	}
}

func InLimitRead(p bool) ReadOpt {
	return func(args *ReadOpts) {
		args.InLimitRead = p
	}
}

// InJQ sets the jq expression.
func InJQ(p string) ReadOpt {
	return func(args *ReadOpts) {
		args.InJQuery = p
	}
}

// InSkip sets the number of rows to skip.
func InSkip(s int) ReadOpt {
	return func(args *ReadOpts) {
		args.InSkip = s
	}
}

// InDelimiter is the field delimiter.
func InDelimiter(d string) ReadOpt {
	return func(args *ReadOpts) {
		args.InDelimiter = d
	}
}

// InHeader is true if there is a header.
func InHeader(h bool) ReadOpt {
	return func(args *ReadOpts) {
		args.InHeader = h
	}
}

// InNeedNULL sets whether InNULL values should be replaced with NULL.
func InNeedNULL(n bool) ReadOpt {
	return func(args *ReadOpts) {
		args.InNeedNULL = n
	}
}

// InNULL sets the input string to replace with NULL.
func InNULL(s string) ReadOpt {
	return func(args *ReadOpts) {
		args.InNULL = s
	}
}

// IsTemporary sets whether to create a temporary table.
func IsTemporary(t bool) ReadOpt {
	return func(args *ReadOpts) {
		args.IsTemporary = t
	}
}

func InRowNumber(t bool) ReadOpt {
	return func(args *ReadOpts) {
		args.InRowNumber = t
	}
}

// InBatchTable sets the target table name for batch import.
func InBatchTable(table string) ReadOpt {
	return func(args *ReadOpts) {
		args.InBatchTable = table
	}
}

// InBatchID sets the batch identity of the delivered file.
// Setting it enables batch aware import.
func InBatchID(id string) ReadOpt {
	return func(args *ReadOpts) {
		args.InBatchID = id
	}
}

// InBatchColumn sets the name of the column that stores
// the batch identity on each imported row.
func InBatchColumn(column string) ReadOpt {
	return func(args *ReadOpts) {
		args.InBatchColumn = column
	}
}

// InBatchMode sets how a revised batch is handled.
func InBatchMode(mode BatchMode) ReadOpt {
	return func(args *ReadOpts) {
		args.InBatchMode = mode
	}
}

// NewReader returns a Reader interface
// depending on the file to be imported.
func NewReader(reader io.Reader, readOpts *ReadOpts) (Reader, error) {
	if reader == nil {
		return nil, ErrNoReader
	}
	readerFunc, ok := readerFuncs[readOpts.realFormat]
	if !ok {
		return nil, ErrUnknownFormat
	}

	return readerFunc(reader, readOpts)
}

func skipRead(r Reader, skipNum int) {
	skip := make([]any, 1)
	for range skipNum {
		row, err := r.ReadRow(skip)
		if err != nil {
			log.Printf("ERROR: skip error %s", err)
			break
		}
		debug.Printf("Skip row:%s\n", row)
	}
}
