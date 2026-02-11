package storage

import (
	"encoding/binary"
)

// PageSize est la taille d'une page en octets (4 KB).
const PageSize = 4096

// PageType identifie le type d'une page.
type PageType byte

const (
	PageTypeMeta     PageType = 1 // page de métadonnées
	PageTypeData     PageType = 2 // page de données (documents)
	PageTypeIndex    PageType = 3 // page d'index
	PageTypeFree     PageType = 4 // page libre
	PageTypeOverflow PageType = 5 // page d'overflow pour gros documents
)

// PageHeader est l'en-tête commun à toute page (16 octets).
// Layout :
//
//	[0]    PageType
//	[1-4]  PageID (uint32)
//	[5-6]  NumRecords (uint16)    — pour data pages
//	[7-8]  FreeSpaceOffset (uint16) — premier octet libre dans la page
//	[9-12] NextPageID (uint32)    — chaînage de pages (0 = aucune)
//	[13-15] réservé
const PageHeaderSize = 16

// Page représente une page brute de 4 KB.
type Page struct {
	Data [PageSize]byte
}

// NewPage crée une page vide avec le type et l'ID donnés.
func NewPage(ptype PageType, pageID uint32) *Page {
	p := &Page{}
	p.Data[0] = byte(ptype)
	binary.LittleEndian.PutUint32(p.Data[1:5], pageID)
	// FreeSpaceOffset commence juste après le header
	binary.LittleEndian.PutUint16(p.Data[7:9], PageHeaderSize)
	return p
}

// Type retourne le type de la page.
func (p *Page) Type() PageType {
	return PageType(p.Data[0])
}

// PageID retourne l'identifiant de la page.
func (p *Page) PageID() uint32 {
	return binary.LittleEndian.Uint32(p.Data[1:5])
}

// NumRecords retourne le nombre de records dans la page.
func (p *Page) NumRecords() uint16 {
	return binary.LittleEndian.Uint16(p.Data[5:7])
}

// SetNumRecords met à jour le nombre de records.
func (p *Page) SetNumRecords(n uint16) {
	binary.LittleEndian.PutUint16(p.Data[5:7], n)
}

// FreeSpaceOffset retourne l'offset du premier octet libre.
func (p *Page) FreeSpaceOffset() uint16 {
	return binary.LittleEndian.Uint16(p.Data[7:9])
}

// SetFreeSpaceOffset met à jour l'offset d'espace libre.
func (p *Page) SetFreeSpaceOffset(off uint16) {
	binary.LittleEndian.PutUint16(p.Data[7:9], off)
}

// NextPageID retourne l'ID de la page suivante chaînée.
func (p *Page) NextPageID() uint32 {
	return binary.LittleEndian.Uint32(p.Data[9:13])
}

// SetNextPageID définit l'ID de la page suivante.
func (p *Page) SetNextPageID(id uint32) {
	binary.LittleEndian.PutUint32(p.Data[9:13], id)
}

// FreeSpace retourne l'espace libre disponible dans la page.
func (p *Page) FreeSpace() int {
	return PageSize - int(p.FreeSpaceOffset())
}

// Constantes pour les flags de slot.
const (
	SlotFlagActive       byte = 0x00 // record actif, données dans la page
	SlotFlagDeleted      byte = 0x01 // record supprimé
	SlotFlagOverflow     byte = 0x02 // record actif, données dans des overflow pages
	SlotFlagDelOver      byte = 0x03 // record supprimé qui avait des overflow pages
	SlotFlagCompressed   byte = 0x04 // record actif, données compressées (snappy)
	SlotFlagCompOverflow byte = 0x06 // record actif, overflow + compressé
)

// OverflowSlotSize est la taille d'un slot d'overflow pointer dans une data page.
// Format : [record_id:8][data_len=8:2][flags=0x02:1][total_len:4][overflow_page:4]
const OverflowSlotSize = 8 + 2 + 1 + 4 + 4 // = 19 bytes

// OverflowDataCapacity est la capacité de données brutes par overflow page.
const OverflowDataCapacity = PageSize - PageHeaderSize // = 4080 bytes

// AppendRecord ajoute un record binaire dans la page.
// Format d'un slot : [record_id:uint64][data_len:uint16][flags:byte][data_bytes...]
// Retourne false si pas assez de place.
const RecordSlotHeaderSize = 8 + 2 + 1 // record_id + data_len + flags

func (p *Page) AppendRecord(recordID uint64, data []byte) bool {
	return p.AppendRecordWithFlag(recordID, data, SlotFlagActive)
}

// AppendRecordWithFlag ajoute un record avec un flag personnalisé (ex: SlotFlagCompressed).
func (p *Page) AppendRecordWithFlag(recordID uint64, data []byte, flag byte) bool {
	needed := RecordSlotHeaderSize + len(data)
	if p.FreeSpace() < needed {
		return false
	}
	off := p.FreeSpaceOffset()
	binary.LittleEndian.PutUint64(p.Data[off:], recordID)
	binary.LittleEndian.PutUint16(p.Data[off+8:], uint16(len(data)))
	p.Data[off+10] = flag
	copy(p.Data[off+11:], data)

	p.SetFreeSpaceOffset(off + uint16(needed))
	p.SetNumRecords(p.NumRecords() + 1)
	return true
}

// AppendOverflowPointer ajoute un slot de pointeur overflow dans la page.
// Le slot contient totalLen (taille totale du document) et firstOverflowPage.
func (p *Page) AppendOverflowPointer(recordID uint64, totalLen uint32, firstOverflowPage uint32) bool {
	if p.FreeSpace() < OverflowSlotSize {
		return false
	}
	off := p.FreeSpaceOffset()
	binary.LittleEndian.PutUint64(p.Data[off:], recordID)
	binary.LittleEndian.PutUint16(p.Data[off+8:], 8) // data_len = 8 (totalLen + pageID)
	p.Data[off+10] = SlotFlagOverflow
	binary.LittleEndian.PutUint32(p.Data[off+11:], totalLen)
	binary.LittleEndian.PutUint32(p.Data[off+15:], firstOverflowPage)

	p.SetFreeSpaceOffset(off + OverflowSlotSize)
	p.SetNumRecords(p.NumRecords() + 1)
	return true
}

// WriteOverflowData écrit des données brutes dans une overflow page (après le header).
func (p *Page) WriteOverflowData(data []byte) {
	copy(p.Data[PageHeaderSize:], data)
}

// ReadOverflowData lit les données brutes d'une overflow page.
func (p *Page) ReadOverflowData(length int) []byte {
	if length > OverflowDataCapacity {
		length = OverflowDataCapacity
	}
	out := make([]byte, length)
	copy(out, p.Data[PageHeaderSize:])
	return out
}

// RecordSlot représente un record lu depuis une page.
type RecordSlot struct {
	RecordID   uint64
	Data       []byte
	Deleted    bool
	Overflow   bool   // true si les données sont dans des overflow pages
	Compressed bool   // true si les données sont compressées (snappy)
	Offset     uint16 // offset dans la page (pour mise à jour)
}

// OverflowInfo extrait totalLen et firstOverflowPageID d'un slot overflow.
func (s *RecordSlot) OverflowInfo() (totalLen uint32, firstPage uint32) {
	if len(s.Data) < 8 {
		return 0, 0
	}
	totalLen = binary.LittleEndian.Uint32(s.Data[0:4])
	firstPage = binary.LittleEndian.Uint32(s.Data[4:8])
	return
}

// ReadRecords lit tous les records non-supprimés de la page.
// Les slots avec SlotFlagOverflow ont Data = [totalLen:4][overflowPageID:4].
func (p *Page) ReadRecords() []RecordSlot {
	slots := make([]RecordSlot, 0, p.NumRecords())
	off := uint16(PageHeaderSize)
	end := p.FreeSpaceOffset()

	for off < end {
		if off+RecordSlotHeaderSize > end {
			break
		}
		rid := binary.LittleEndian.Uint64(p.Data[off:])
		dlen := binary.LittleEndian.Uint16(p.Data[off+8:])
		flags := p.Data[off+10]

		dataStart := off + RecordSlotHeaderSize
		if int(dataStart)+int(dlen) > PageSize {
			break
		}
		dataCopy := make([]byte, dlen)
		copy(dataCopy, p.Data[dataStart:dataStart+dlen])

		slots = append(slots, RecordSlot{
			RecordID:   rid,
			Data:       dataCopy,
			Deleted:    flags == SlotFlagDeleted || flags == SlotFlagDelOver,
			Overflow:   flags == SlotFlagOverflow || flags == SlotFlagCompOverflow,
			Compressed: flags == SlotFlagCompressed || flags == SlotFlagCompOverflow,
			Offset:     off,
		})
		off = dataStart + dlen
	}
	return slots
}

// MarkDeleted marque un record comme supprimé à l'offset donné.
// Préserve le flag overflow pour permettre la libération des overflow pages.
func (p *Page) MarkDeleted(slotOffset uint16) {
	flag := p.Data[slotOffset+10]
	if flag == SlotFlagOverflow || flag == SlotFlagCompOverflow {
		p.Data[slotOffset+10] = SlotFlagDelOver
	} else {
		p.Data[slotOffset+10] = SlotFlagDeleted
	}
}

// SlotFlags retourne le flag brut d'un slot à l'offset donné.
func (p *Page) SlotFlags(slotOffset uint16) byte {
	return p.Data[slotOffset+10]
}

// UpdateRecordInPlace met à jour les données d'un record si la nouvelle taille
// est identique à l'ancienne. Retourne false si la taille diffère.
func (p *Page) UpdateRecordInPlace(slotOffset uint16, newData []byte) bool {
	oldLen := binary.LittleEndian.Uint16(p.Data[slotOffset+8:])
	if uint16(len(newData)) != oldLen {
		return false
	}
	copy(p.Data[slotOffset+RecordSlotHeaderSize:], newData)
	return true
}
