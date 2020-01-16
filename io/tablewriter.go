package io

import (
	"os"

	"github.com/olekukonko/tablewriter"
)

type Row interface {
	Headers() []string
	FieldValues() []string
}

type TableWriter struct {
	rows []Row
}

func (w *TableWriter) AddRow(row Row) {
	w.rows = append(w.rows, row)
}

func (w *TableWriter) Render() {
	if len(w.rows) == 0 {
		return
	}
	table := tablewriter.NewWriter(os.Stdout)
	table.SetHeader(w.rows[0].Headers())
	for _, row := range w.rows {
		table.Append(row.FieldValues())
	}
	table.Render()
}
