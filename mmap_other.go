//go:build !linux

package hashtabledb

func (db *DB) mapMainFile() error {
	return nil
}

func (db *DB) unmapMainFile() error {
	return nil
}
