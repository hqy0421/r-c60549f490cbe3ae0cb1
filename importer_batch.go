package trdsql

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
)

// BatchMode determines how a revised batch is handled.
// A revised batch is a delivery whose batch identity already
// exists in the target table but whose content differs.
type BatchMode int

const (
	// BatchReplace replaces the existing rows of the batch
	// with the revised content (default).
	BatchReplace BatchMode = iota
	// BatchError rejects a revised batch with ErrBatchConflict.
	BatchError
)

// String returns the string representation of the BatchMode.
func (m BatchMode) String() string {
	switch m {
	case BatchReplace:
		return "replace"
	case BatchError:
		return "error"
	default:
		return "Unknown"
	}
}

// ErrBatchConflict is returned when a revised batch (same batch
// identity with different content) is delivered in BatchError mode.
var ErrBatchConflict = errors.New("batch conflict: same batch identity with different content")

const (
	// DefaultBatchColumn is the default name of the column
	// that stores the batch identity on each imported row.
	DefaultBatchColumn = "_batch_id"
	// batchRegistryTable is the name of the table that records
	// the fingerprint of every imported batch.
	batchRegistryTable = "_trdsql_batch_registry"
)

// importBatchFileContext imports a file as a batch.
//
// The batch identity (opts.InBatchID) and the fingerprint of the
// delivered content are recorded in the batch registry, and every
// imported row is tagged with the same identity, so the import and
// the visible scope of the batch in the target table use one
// consistent batch identity:
//
//   - exact duplicate (same identity, same content): nothing is
//     written again; the rows already in the target table are used.
//   - revised (same identity, different content): the rows of this
//     batch are replaced according to opts.InBatchMode.
//   - otherwise: the batch is appended to the target table.
//
// Rows of other batches are not touched and remain available as
// report input. The whole operation runs inside the current
// transaction, so a failed delivery leaves the target unchanged.
func importBatchFileContext(ctx context.Context, db *DB, file io.Reader, fileName string, opts *ReadOpts) (string, error) {
	// The delivered content is buffered to compute its fingerprint.
	content, err := io.ReadAll(file)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(content)
	fingerprint := hex.EncodeToString(sum[:])

	tableName := opts.InBatchTable
	if tableName == "" {
		tableName = fileName
		if opts.InJQuery != "" {
			tableName = fmt.Sprintf("%s::jq%d", fileName, db.importCount)
		}
	}
	quotedName := db.QuotedName(tableName)

	if err := db.ensureBatchRegistry(ctx); err != nil {
		return quotedName, fmt.Errorf("batch registry: %w", err)
	}

	current, found, err := db.batchFingerprint(ctx, tableName, opts.InBatchID)
	if err != nil {
		return quotedName, err
	}

	if found && current == fingerprint {
		// Exact duplicate delivery: the batch is already in the
		// target table. Do not write it again.
		debug.Printf("batch %q for table %s is identical: skip import", opts.InBatchID, tableName)
		return quotedName, nil
	}

	if found && opts.InBatchMode == BatchError {
		return quotedName, fmt.Errorf("%w (table %s, batch %q)", ErrBatchConflict, tableName, opts.InBatchID)
	}

	reader, err := NewReader(bytes.NewReader(content), opts)
	if err != nil {
		return quotedName, err
	}

	if opts.InRowNumber {
		reader = newRowNumberReader(reader)
	}
	batch := newBatchReader(reader, opts.InBatchColumn, opts.InBatchID)

	columnNames, err := batch.Names()
	if err != nil {
		if !errors.Is(err, io.EOF) {
			return quotedName, err
		}
		debug.Printf("EOF reached before argument number of rows")
	}
	columnTypes, err := batch.Types()
	if err != nil {
		if !errors.Is(err, io.EOF) {
			return quotedName, err
		}
		debug.Printf("EOF reached before argument number of rows")
	}
	debug.Printf("Column Names: [%v]", strings.Join(columnNames, ","))
	debug.Printf("Column Types: [%v]", strings.Join(columnTypes, ","))

	if err := db.createBatchTableContext(ctx, quotedName, columnNames, columnTypes); err != nil {
		return quotedName, err
	}

	if found {
		// Revised delivery: replace the visible scope of this batch.
		debug.Printf("batch %q for table %s revised: replace existing rows", opts.InBatchID, tableName)
		if err := db.deleteBatch(ctx, quotedName, batch.column, opts.InBatchID); err != nil {
			return quotedName, fmt.Errorf("batch replace: %w", err)
		}
	}

	if err := db.ImportContext(ctx, quotedName, columnNames, batch); err != nil {
		return quotedName, err
	}

	if err := db.registerBatch(ctx, tableName, opts.InBatchID, fingerprint); err != nil {
		return quotedName, fmt.Errorf("batch register: %w", err)
	}
	return quotedName, nil
}

// batchReader is a Reader that tags every row with the batch identity.
type batchReader struct {
	reader    Reader
	column    string
	id        string
	originRow []any
}

// newBatchReader creates a new batchReader.
// The batch column name is made unique against the existing
// column names by appending "_" until it does not conflict.
func newBatchReader(r Reader, column string, id string) *batchReader {
	names, err := r.Names()
	if err != nil {
		names = nil
	}
	for column == "" || slicesContains(names, column) {
		column += "_"
	}
	return &batchReader{
		reader:    r,
		column:    column,
		id:        id,
		originRow: make([]any, len(names)),
	}
}

func slicesContains(names []string, name string) bool {
	for _, n := range names {
		if n == name {
			return true
		}
	}
	return false
}

// Names returns column names with an additional batch identity column.
func (r *batchReader) Names() ([]string, error) {
	names, err := r.reader.Names()
	if err != nil {
		return nil, err
	}
	return append(append([]string{}, names...), r.column), nil
}

// Types returns column types with an additional batch identity column.
func (r *batchReader) Types() ([]string, error) {
	types, err := r.reader.Types()
	if err != nil {
		return nil, err
	}
	return append(append([]string{}, types...), DefaultDBType), nil
}

// PreReadRow returns pre-read rows with an additional batch identity column.
func (r *batchReader) PreReadRow() [][]any {
	preReadRows := r.reader.PreReadRow()
	for i := range preReadRows {
		preReadRows[i] = append(append([]any{}, preReadRows[i]...), r.id)
	}
	return preReadRows
}

// ReadRow reads the rest of the row with an additional batch identity column.
func (r *batchReader) ReadRow(row []any) ([]any, error) {
	var err error
	r.originRow, err = r.reader.ReadRow(r.originRow)
	if err != nil {
		return nil, err
	}
	if len(r.originRow) == 0 {
		return nil, nil
	}
	return append(append([]any{}, r.originRow...), r.id), nil
}

// ensureBatchRegistry creates the batch registry table if it does not exist.
func (db *DB) ensureBatchRegistry(ctx context.Context) error {
	if db.Tx == nil {
		return ErrNoTransaction
	}
	query := "CREATE TABLE IF NOT EXISTS " + db.QuotedName(batchRegistryTable) + " (" +
		db.QuotedName("table_name") + " VARCHAR(512) NOT NULL, " +
		db.QuotedName("batch_id") + " VARCHAR(255) NOT NULL, " +
		db.QuotedName("content_hash") + " VARCHAR(64) NOT NULL, " +
		"PRIMARY KEY (" + db.QuotedName("table_name") + ", " + db.QuotedName("batch_id") + "))"
	debug.Print(query)
	_, err := db.Tx.ExecContext(ctx, query)
	return err
}

// batchFingerprint returns the recorded content fingerprint
// of the batch identity in the target table.
func (db *DB) batchFingerprint(ctx context.Context, tableName string, batchID string) (string, bool, error) {
	query := db.rebind("SELECT " + db.QuotedName("content_hash") +
		" FROM " + db.QuotedName(batchRegistryTable) +
		" WHERE " + db.QuotedName("table_name") + " = ? AND " + db.QuotedName("batch_id") + " = ?")
	debug.Print(query)
	var fingerprint string
	err := db.Tx.QueryRowContext(ctx, query, tableName, batchID).Scan(&fingerprint)
	if errors.Is(err, sql.ErrNoRows) {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	return fingerprint, true, nil
}

// registerBatch records the content fingerprint of the batch identity.
func (db *DB) registerBatch(ctx context.Context, tableName string, batchID string, fingerprint string) error {
	update := db.rebind("UPDATE " + db.QuotedName(batchRegistryTable) +
		" SET " + db.QuotedName("content_hash") + " = ?" +
		" WHERE " + db.QuotedName("table_name") + " = ? AND " + db.QuotedName("batch_id") + " = ?")
	debug.Print(update)
	result, err := db.Tx.ExecContext(ctx, update, fingerprint, tableName, batchID)
	if err != nil {
		return err
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if affected > 0 {
		return nil
	}
	insert := db.rebind("INSERT INTO " + db.QuotedName(batchRegistryTable) +
		" (" + db.QuotedName("table_name") + ", " + db.QuotedName("batch_id") + ", " + db.QuotedName("content_hash") + ")" +
		" VALUES (?, ?, ?)")
	debug.Print(insert)
	_, err = db.Tx.ExecContext(ctx, insert, tableName, batchID, fingerprint)
	return err
}

// deleteBatch deletes the visible scope of the batch identity
// from the target table.
func (db *DB) deleteBatch(ctx context.Context, quotedTable string, column string, batchID string) error {
	query := db.rebind("DELETE FROM " + quotedTable +
		" WHERE " + db.QuotedName(column) + " = ?")
	debug.Print(query)
	_, err := db.Tx.ExecContext(ctx, query, batchID)
	return err
}

// createBatchTableContext creates the batch target table if it does not exist.
// The table is always persistent (never temporary) so that the batch
// identity of imported rows survives across deliveries.
func (db *DB) createBatchTableContext(ctx context.Context, quotedTable string, columnNames []string, columnTypes []string) error {
	if db.Tx == nil {
		return ErrNoTransaction
	}
	if len(columnNames) == 0 {
		return ErrInvalidNames
	}
	if len(columnNames) != len(columnTypes) {
		return ErrInvalidTypes
	}

	var buf strings.Builder
	buf.WriteString("CREATE TABLE IF NOT EXISTS ")
	buf.WriteString(quotedTable)
	buf.WriteString(" ( ")
	buf.WriteString(db.QuotedName(columnNames[0]))
	buf.WriteString(" ")
	buf.WriteString(columnTypes[0])
	for i, columnName := range columnNames[1:] {
		buf.WriteString(", ")
		buf.WriteString(db.QuotedName(columnName))
		buf.WriteString(" ")
		buf.WriteString(columnTypes[i+1])
	}
	buf.WriteString(" );")
	query := buf.String()
	debug.Print(query)
	_, err := db.Tx.ExecContext(ctx, query)
	return err
}

// rebind replaces ? placeholders with driver specific placeholders.
func (db *DB) rebind(query string) string {
	if db.driver != "postgres" {
		return query
	}
	var buf strings.Builder
	n := 0
	for _, r := range query {
		if r == '?' {
			n++
			buf.WriteString("$" + strconv.Itoa(n))
		} else {
			buf.WriteRune(r)
		}
	}
	return buf.String()
}
