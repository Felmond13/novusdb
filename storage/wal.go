package storage

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"sync"
)

// WALRecordType identifie le type d'opération enregistrée dans le WAL.
type WALRecordType byte

const (
	WALPageWrite  WALRecordType = 1 // écriture d'une page complète
	WALCommit     WALRecordType = 2 // marqueur de commit
	WALCheckpoint WALRecordType = 3 // marqueur de checkpoint terminé
)

// walFileHeader est l'en-tête du fichier WAL (16 octets).
// [0-3]  magic number ("DWAL")
// [4-7]  version (uint32)
// [8-15] réservé
const walHeaderSize = 16

var walMagic = [4]byte{'D', 'W', 'A', 'L'}

// WALRecord représente une entrée dans le Write-Ahead Log.
//
// Format sur disque :
//
//	[LSN:uint64][Type:byte][PageID:uint32][DataLen:uint32][Data:bytes][CRC32:uint32]
//
// Pour un WALCommit, DataLen=0 et pas de Data.
const walRecordHeaderSize = 8 + 1 + 4 + 4 // LSN + Type + PageID + DataLen
const walRecordCRCSize = 4

type WALRecord struct {
	LSN    uint64
	Type   WALRecordType
	PageID uint32
	Data   []byte // before-image ou after-image de la page
}

// WAL gère le Write-Ahead Log pour la durabilité.
type WAL struct {
	mu       sync.Mutex
	file     *os.File
	path     string
	nextLSN  uint64
	synced   bool // true si le dernier write a été fsync-é
	records  []WALRecord
	commitLSN uint64 // dernier LSN commité
}

// OpenWAL ouvre ou crée le fichier WAL associé à la base de données.
// Le chemin du WAL est le chemin de la base + ".wal".
func OpenWAL(dbPath string) (*WAL, error) {
	walPath := dbPath + ".wal"
	file, err := os.OpenFile(walPath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, fmt.Errorf("wal: cannot open file: %w", err)
	}

	w := &WAL{
		file:    file,
		path:    walPath,
		nextLSN: 1,
	}

	info, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, err
	}

	if info.Size() == 0 {
		// Nouveau WAL : écrire le header
		if err := w.writeHeader(); err != nil {
			file.Close()
			return nil, err
		}
	} else {
		// WAL existant : vérifier le header et charger les records
		if err := w.readHeader(); err != nil {
			file.Close()
			return nil, err
		}
		if err := w.loadRecords(); err != nil {
			file.Close()
			return nil, err
		}
	}

	return w, nil
}

// Close ferme le fichier WAL.
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.file.Close()
}

// LogPageWrite enregistre l'écriture d'une page (before-image pour undo, after-image pour redo).
// Ici on stocke l'after-image (la page telle qu'elle sera après l'opération).
func (w *WAL) LogPageWrite(pageID uint32, afterImage []byte) (uint64, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	lsn := w.nextLSN
	w.nextLSN++

	rec := WALRecord{
		LSN:    lsn,
		Type:   WALPageWrite,
		PageID: pageID,
		Data:   make([]byte, len(afterImage)),
	}
	copy(rec.Data, afterImage)

	if err := w.appendRecord(&rec); err != nil {
		return 0, err
	}

	w.records = append(w.records, rec)
	w.synced = false
	return lsn, nil
}

// Commit écrit un marqueur de commit et fait un fsync.
// Après cet appel, toutes les opérations précédentes sont durables.
func (w *WAL) Commit() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	lsn := w.nextLSN
	w.nextLSN++

	rec := WALRecord{
		LSN:  lsn,
		Type: WALCommit,
	}

	if err := w.appendRecord(&rec); err != nil {
		return err
	}

	// fsync — c'est LE moment critique qui garantit la durabilité
	if err := w.file.Sync(); err != nil {
		return fmt.Errorf("wal: fsync commit: %w", err)
	}

	w.commitLSN = lsn
	w.records = append(w.records, rec)
	w.synced = true
	return nil
}

// Sync force un fsync du WAL sans écrire de marqueur commit.
func (w *WAL) Sync() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.file.Sync()
}

// CommittedPageWrites retourne les WALPageWrite records qui ont été commités,
// dans l'ordre chronologique. Utilisé pour le recovery et le checkpoint.
func (w *WAL) CommittedPageWrites() []WALRecord {
	w.mu.Lock()
	defer w.mu.Unlock()

	var committed []WALRecord
	var pending []WALRecord

	for _, r := range w.records {
		switch r.Type {
		case WALPageWrite:
			pending = append(pending, r)
		case WALCommit:
			committed = append(committed, pending...)
			pending = nil
		}
	}
	// Les pending sans commit sont ignorés (transaction non terminée)
	return committed
}

// HasUncommittedWrites retourne true s'il y a des écritures non commitées dans le WAL.
func (w *WAL) HasUncommittedWrites() bool {
	w.mu.Lock()
	defer w.mu.Unlock()

	for i := len(w.records) - 1; i >= 0; i-- {
		switch w.records[i].Type {
		case WALPageWrite:
			return true // on a trouvé un write sans commit après
		case WALCommit:
			return false // le dernier élément significatif est un commit
		}
	}
	return false
}

// Truncate vide le WAL après un checkpoint réussi.
// Réécrit juste le header, effaçant tous les records.
func (w *WAL) Truncate() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if err := w.file.Truncate(walHeaderSize); err != nil {
		return fmt.Errorf("wal: truncate: %w", err)
	}
	if _, err := w.file.Seek(walHeaderSize, io.SeekStart); err != nil {
		return fmt.Errorf("wal: seek after truncate: %w", err)
	}
	if err := w.file.Sync(); err != nil {
		return fmt.Errorf("wal: fsync after truncate: %w", err)
	}

	w.records = nil
	w.commitLSN = 0
	return nil
}

// RecordCount retourne le nombre de records dans le WAL.
func (w *WAL) RecordCount() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return len(w.records)
}

// --- Méthodes internes ---

func (w *WAL) writeHeader() error {
	var hdr [walHeaderSize]byte
	copy(hdr[0:4], walMagic[:])
	binary.LittleEndian.PutUint32(hdr[4:8], 1) // version 1
	_, err := w.file.WriteAt(hdr[:], 0)
	return err
}

func (w *WAL) readHeader() error {
	var hdr [walHeaderSize]byte
	if _, err := w.file.ReadAt(hdr[:], 0); err != nil {
		return fmt.Errorf("wal: read header: %w", err)
	}
	if hdr[0] != walMagic[0] || hdr[1] != walMagic[1] || hdr[2] != walMagic[2] || hdr[3] != walMagic[3] {
		return fmt.Errorf("wal: invalid magic number")
	}
	version := binary.LittleEndian.Uint32(hdr[4:8])
	if version != 1 {
		return fmt.Errorf("wal: unsupported version %d", version)
	}
	return nil
}

func (w *WAL) appendRecord(rec *WALRecord) error {
	// Calculer la taille totale
	dataLen := len(rec.Data)
	totalSize := walRecordHeaderSize + dataLen + walRecordCRCSize
	buf := make([]byte, totalSize)

	// Écrire le header
	off := 0
	binary.LittleEndian.PutUint64(buf[off:], rec.LSN)
	off += 8
	buf[off] = byte(rec.Type)
	off++
	binary.LittleEndian.PutUint32(buf[off:], rec.PageID)
	off += 4
	binary.LittleEndian.PutUint32(buf[off:], uint32(dataLen))
	off += 4

	// Écrire les données
	if dataLen > 0 {
		copy(buf[off:], rec.Data)
		off += dataLen
	}

	// CRC32 sur tout le record (hors CRC lui-même)
	crc := crc32.ChecksumIEEE(buf[:off])
	binary.LittleEndian.PutUint32(buf[off:], crc)

	// Append au fichier (seek to end)
	if _, err := w.file.Seek(0, io.SeekEnd); err != nil {
		return fmt.Errorf("wal: seek end: %w", err)
	}
	if _, err := w.file.Write(buf); err != nil {
		return fmt.Errorf("wal: write record: %w", err)
	}
	return nil
}

func (w *WAL) loadRecords() error {
	w.records = nil

	offset := int64(walHeaderSize)
	hdrBuf := make([]byte, walRecordHeaderSize)

	for {
		// Lire le header du record
		n, err := w.file.ReadAt(hdrBuf, offset)
		if err == io.EOF || n < walRecordHeaderSize {
			break // fin du fichier ou record incomplet
		}
		if err != nil {
			return fmt.Errorf("wal: read record header at offset %d: %w", offset, err)
		}

		lsn := binary.LittleEndian.Uint64(hdrBuf[0:8])
		rtype := WALRecordType(hdrBuf[8])
		pageID := binary.LittleEndian.Uint32(hdrBuf[9:13])
		dataLen := binary.LittleEndian.Uint32(hdrBuf[13:17])

		// Lire les données + CRC
		remaining := int(dataLen) + walRecordCRCSize
		dataBuf := make([]byte, remaining)
		n, err = w.file.ReadAt(dataBuf, offset+int64(walRecordHeaderSize))
		if err == io.EOF || n < remaining {
			break // record incomplet (crash pendant écriture) — on s'arrête ici
		}
		if err != nil {
			return fmt.Errorf("wal: read record data at offset %d: %w", offset, err)
		}

		// Vérifier le CRC
		crcOffset := int(dataLen)
		storedCRC := binary.LittleEndian.Uint32(dataBuf[crcOffset:])

		// Reconstituer le buffer pour le calcul CRC
		fullBuf := make([]byte, walRecordHeaderSize+int(dataLen))
		copy(fullBuf, hdrBuf)
		copy(fullBuf[walRecordHeaderSize:], dataBuf[:dataLen])
		computedCRC := crc32.ChecksumIEEE(fullBuf)

		if storedCRC != computedCRC {
			// CRC invalide — record corrompu, on s'arrête ici (crash recovery safe)
			break
		}

		var data []byte
		if dataLen > 0 {
			data = make([]byte, dataLen)
			copy(data, dataBuf[:dataLen])
		}

		rec := WALRecord{
			LSN:    lsn,
			Type:   rtype,
			PageID: pageID,
			Data:   data,
		}
		w.records = append(w.records, rec)

		if lsn >= w.nextLSN {
			w.nextLSN = lsn + 1
		}
		if rtype == WALCommit && lsn > w.commitLSN {
			w.commitLSN = lsn
		}

		offset += int64(walRecordHeaderSize) + int64(remaining)
	}

	return nil
}
