package tables

import (
	"fmt"
	"github.com/ngaut/log"
)

func debugTable(t *Table) {
	cols := fmt.Sprintf("table: %s, cols: ", t.Name.O)
	wcols := fmt.Sprintf("table: %s, wcols: ", t.Name.O)
	for _, col := range t.Columns {
		cols += fmt.Sprintf("(%s,%d), ", col.Name.O, col.Offset)
	}
	for _, col := range t.WritableCols() {
		wcols += fmt.Sprintf("(%s,%d), ", col.Name.O, col.Offset)
	}
	log.Errorf("%s", cols)
	log.Errorf("%s", wcols)
}
