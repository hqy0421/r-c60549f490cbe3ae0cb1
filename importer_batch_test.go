package trdsql

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

// writeDelivery writes a delivered CSV archive to a file.
func writeDelivery(t *testing.T, dir string, name string, content string) string {
	t.Helper()
	fileName := filepath.Join(dir, name)
	if err := os.WriteFile(fileName, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return fileName
}

// deliver imports one delivery as one run and returns the target table name.
func deliver(t *testing.T, dsn string, fileName string, opts *ReadOpts) (string, error) {
	t.Helper()
	db, err := Connect(DefaultDriver, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := db.Disconnect(); err != nil {
			t.Fatal(err)
		}
	}()
	db.Tx, err = db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	tableName, err := ImportFile(db, fileName, opts)
	if err != nil {
		if rbErr := db.Tx.Rollback(); rbErr != nil {
			t.Fatal(rbErr)
		}
		return tableName, err
	}
	if err := db.Tx.Commit(); err != nil {
		t.Fatal(err)
	}
	return tableName, nil
}

// queryStrings returns the rows of the query as strings.
func queryStrings(t *testing.T, dsn string, query string) [][]string {
	t.Helper()
	db, err := Connect(DefaultDriver, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := db.Disconnect(); err != nil {
			t.Fatal(err)
		}
	}()
	rows, err := db.Query(query)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := rows.Close(); err != nil {
			t.Fatal(err)
		}
	}()
	columns, err := rows.Columns()
	if err != nil {
		t.Fatal(err)
	}
	var result [][]string
	for rows.Next() {
		values := make([]any, len(columns))
		scanArgs := make([]any, len(columns))
		for i := range values {
			scanArgs[i] = &values[i]
		}
		if err := rows.Scan(scanArgs...); err != nil {
			t.Fatal(err)
		}
		row := make([]string, len(columns))
		for i, v := range values {
			if v == nil {
				row[i] = "NULL"
				continue
			}
			switch v := v.(type) {
			case []byte:
				row[i] = string(v)
			case string:
				row[i] = v
			default:
				row[i] = fmt.Sprintf("%v", v)
			}
		}
		result = append(result, row)
	}
	return result
}

func batchOpts(table string, id string) *ReadOpts {
	return NewReadOpts(
		InFormat(CSV),
		InHeader(true),
		InBatchTable(table),
		InBatchID(id),
	)
}

func TestBatchImportLifecycle(t *testing.T) {
	dir := t.TempDir()
	dsn := filepath.Join(dir, "prod.db")
	fileName := writeDelivery(t, dir, "prod_001.csv", "id,name\n1,Orange\n2,Melon\n")

	// First delivery: the batch is imported and tagged with its identity.
	tableName, err := deliver(t, dsn, fileName, batchOpts("production", "b001"))
	if err != nil {
		t.Fatal(err)
	}
	if tableName != "`production`" {
		t.Errorf("deliver() tableName = %v, want %v", tableName, "`production`")
	}
	got := queryStrings(t, dsn, "SELECT id, name, _batch_id FROM production ORDER BY id")
	want := [][]string{
		{"1", "Orange", "b001"},
		{"2", "Melon", "b001"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("first delivery = %v, want %v", got, want)
	}

	// Exact duplicate delivery (same batch identity, same content):
	// nothing is written again.
	if _, err := deliver(t, dsn, fileName, batchOpts("production", "b001")); err != nil {
		t.Fatal(err)
	}
	got = queryStrings(t, dsn, "SELECT id, name, _batch_id FROM production ORDER BY id")
	if !reflect.DeepEqual(got, want) {
		t.Errorf("duplicate delivery = %v, want %v (no duplicate rows)", got, want)
	}

	// Revised delivery (same batch identity, different content):
	// the batch is replaced, other rows are untouched.
	writeDelivery(t, dir, "prod_001.csv", "id,name\n1,Orange\n3,Apple\n4,Peach\n")
	if _, err := deliver(t, dsn, fileName, batchOpts("production", "b001")); err != nil {
		t.Fatal(err)
	}
	got = queryStrings(t, dsn, "SELECT id, name, _batch_id FROM production ORDER BY id")
	want = [][]string{
		{"1", "Orange", "b001"},
		{"3", "Apple", "b001"},
		{"4", "Peach", "b001"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("revised delivery = %v, want %v (batch replaced)", got, want)
	}

	// Another batch: appended, the existing batch remains as report input.
	fileName2 := writeDelivery(t, dir, "prod_002.csv", "id,name\n9,Grape\n")
	if _, err := deliver(t, dsn, fileName2, batchOpts("production", "b002")); err != nil {
		t.Fatal(err)
	}
	got = queryStrings(t, dsn, "SELECT id, name, _batch_id FROM production ORDER BY id")
	want = [][]string{
		{"1", "Orange", "b001"},
		{"3", "Apple", "b001"},
		{"4", "Peach", "b001"},
		{"9", "Grape", "b002"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("other batch = %v, want %v (batches coexist)", got, want)
	}

	// Revised delivery in error mode: rejected, the table is unchanged.
	writeDelivery(t, dir, "prod_002.csv", "id,name\n9,Grape\n10,Lemon\n")
	_, err = deliver(t, dsn, fileName2, NewReadOpts(
		InFormat(CSV),
		InHeader(true),
		InBatchTable("production"),
		InBatchID("b002"),
		InBatchMode(BatchError),
	))
	if !errors.Is(err, ErrBatchConflict) {
		t.Errorf("error mode = %v, want %v", err, ErrBatchConflict)
	}
	got = queryStrings(t, dsn, "SELECT id, name, _batch_id FROM production ORDER BY id")
	if !reflect.DeepEqual(got, want) {
		t.Errorf("after rejected delivery = %v, want %v (unchanged)", got, want)
	}
}

func TestBatchImportFileTable(t *testing.T) {
	dir := t.TempDir()
	dsn := filepath.Join(dir, "prod.db")
	fileName := writeDelivery(t, dir, "prod.csv", "id,name\n1,Orange\n")

	// Without InBatchTable, the table name is derived from the file name.
	tableName, err := deliver(t, dsn, fileName, batchOpts("", "b001"))
	if err != nil {
		t.Fatal(err)
	}
	if tableName != "`"+fileName+"`" {
		t.Errorf("deliver() tableName = %v, want %v", tableName, "`"+fileName+"`")
	}
	got := queryStrings(t, dsn, "SELECT id, name, _batch_id FROM `"+fileName+"`")
	want := [][]string{{"1", "Orange", "b001"}}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("file table = %v, want %v", got, want)
	}
}

func TestBatchImportExec(t *testing.T) {
	dir := t.TempDir()
	dsn := filepath.Join(dir, "prod.db")
	fileName := writeDelivery(t, dir, "prod.csv", "id,name\n1,Orange\n2,Melon\n")

	// The query references the delivered file; the import rewrites it
	// to the stable target table.
	run := func(query string) string {
		t.Helper()
		var out strings.Builder
		trd := NewTRDSQL(
			NewImporter(
				InFormat(CSV),
				InHeader(true),
				InBatchTable("production"),
				InBatchID("b001"),
			),
			NewExporter(NewWriter(OutFormat(CSV), OutStream(&out))),
		)
		trd.Dsn = dsn
		if err := trd.Exec(query); err != nil {
			t.Fatal(err)
		}
		return out.String()
	}

	got := run("SELECT name FROM " + fileName + " ORDER BY id")
	want := "Orange\nMelon\n"
	if got != want {
		t.Errorf("exec = %q, want %q", got, want)
	}

	// Report-only: the target table is queried directly without a delivery.
	// The batch identity is visible in the target scope.
	var out strings.Builder
	trd := NewTRDSQL(nil, NewExporter(NewWriter(OutFormat(CSV), OutStream(&out))))
	trd.Dsn = dsn
	if err := trd.Exec("SELECT _batch_id, COUNT(*) FROM production GROUP BY _batch_id"); err != nil {
		t.Fatal(err)
	}
	if got, want := out.String(), "b001,2\n"; got != want {
		t.Errorf("report = %q, want %q", got, want)
	}
}

func TestBatchImportRegistry(t *testing.T) {
	dir := t.TempDir()
	dsn := filepath.Join(dir, "prod.db")
	fileName := writeDelivery(t, dir, "prod.csv", "id,name\n1,Orange\n")

	if _, err := deliver(t, dsn, fileName, batchOpts("production", "b001")); err != nil {
		t.Fatal(err)
	}
	// The registry records the batch identity with the content fingerprint.
	got := queryStrings(t, dsn,
		"SELECT table_name, batch_id, LENGTH(content_hash) FROM "+batchRegistryTable)
	want := [][]string{{"production", "b001", "64"}}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("registry = %v, want %v", got, want)
	}
}

func Test_newBatchReader(t *testing.T) {
	csvReader := func() Reader {
		r, err := NewCSVReader(strings.NewReader("id,_batch_id\n1,x\n2,y\n"),
			NewReadOpts(InFormat(CSV), InHeader(true), InPreRead(2)))
		if err != nil {
			t.Fatal(err)
		}
		return r
	}

	// The batch column is made unique against existing column names.
	r := newBatchReader(csvReader(), DefaultBatchColumn, "b001")
	names, err := r.Names()
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"id", "_batch_id", "_batch_id_"}; !reflect.DeepEqual(names, want) {
		t.Errorf("Names() = %v, want %v", names, want)
	}
	types, err := r.Types()
	if err != nil {
		t.Fatal(err)
	}
	if len(types) != 3 {
		t.Errorf("Types() = %v, want 3 types", types)
	}
	preRows := r.PreReadRow()
	if len(preRows) != 1 || !reflect.DeepEqual(preRows[0], []any{"1", "x", "b001"}) {
		t.Errorf("PreReadRow() = %v, want row tagged with b001", preRows)
	}
	row, err := r.ReadRow(make([]any, 3))
	if err != nil {
		t.Fatal(err)
	}
	if want := []any{"2", "y", "b001"}; !reflect.DeepEqual(row, want) {
		t.Errorf("ReadRow() = %v, want %v", row, want)
	}
}

func Test_rebind(t *testing.T) {
	db := &DB{driver: "postgres"}
	query := "SELECT * FROM t WHERE a = ? AND b = ? AND c = ?"
	if got, want := db.rebind(query), "SELECT * FROM t WHERE a = $1 AND b = $2 AND c = $3"; got != want {
		t.Errorf("rebind() = %v, want %v", got, want)
	}
	db = &DB{driver: "sqlite"}
	if got := db.rebind(query); got != query {
		t.Errorf("rebind() = %v, want %v (unchanged)", got, query)
	}
}
