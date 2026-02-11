// Package storage gère le stockage bas-niveau : documents, pages et pager.
package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
)

// FieldType représente le type d'un champ dans un document.
type FieldType byte

const (
	FieldNull     FieldType = 0
	FieldString   FieldType = 1
	FieldInt64    FieldType = 2
	FieldFloat64  FieldType = 3
	FieldBool     FieldType = 4
	FieldDocument FieldType = 5 // document imbriqué
	FieldArray    FieldType = 6 // tableau de valeurs
)

// Field représente un champ nommé dans un document.
type Field struct {
	Name  string
	Type  FieldType
	Value interface{} // string | int64 | float64 | bool | nil | *Document | []interface{}
}

// Document représente un document orienté-champs, stockable en binaire.
type Document struct {
	Fields []Field
}

// NewDocument crée un document vide.
func NewDocument() *Document {
	return &Document{}
}

// Set ajoute ou met à jour un champ dans le document.
func (d *Document) Set(name string, value interface{}) {
	for i, f := range d.Fields {
		if f.Name == name {
			d.Fields[i].Type, d.Fields[i].Value = inferType(value)
			return
		}
	}
	t, v := inferType(value)
	d.Fields = append(d.Fields, Field{Name: name, Type: t, Value: v})
}

// Get retourne la valeur d'un champ, ou nil s'il n'existe pas.
func (d *Document) Get(name string) (interface{}, bool) {
	for _, f := range d.Fields {
		if f.Name == name {
			return f.Value, true
		}
	}
	return nil, false
}

// GetNested retourne la valeur d'un champ imbriqué (ex: "params.timeout").
func (d *Document) GetNested(path []string) (interface{}, bool) {
	if len(path) == 0 {
		return nil, false
	}
	if len(path) == 1 {
		return d.Get(path[0])
	}
	val, ok := d.Get(path[0])
	if !ok {
		return nil, false
	}
	sub, ok := val.(*Document)
	if !ok {
		return nil, false
	}
	return sub.GetNested(path[1:])
}

// SetNested définit la valeur d'un champ imbriqué, créant les sous-documents si nécessaire.
func (d *Document) SetNested(path []string, value interface{}) {
	if len(path) == 0 {
		return
	}
	if len(path) == 1 {
		d.Set(path[0], value)
		return
	}
	val, ok := d.Get(path[0])
	var sub *Document
	if ok {
		sub, ok = val.(*Document)
	}
	if !ok {
		sub = NewDocument()
		d.Set(path[0], sub)
	}
	sub.SetNested(path[1:], value)
}

// inferType déduit le FieldType à partir d'une valeur Go.
func inferType(value interface{}) (FieldType, interface{}) {
	if value == nil {
		return FieldNull, nil
	}
	switch v := value.(type) {
	case string:
		return FieldString, v
	case int:
		return FieldInt64, int64(v)
	case int64:
		return FieldInt64, v
	case float64:
		return FieldFloat64, v
	case bool:
		return FieldBool, v
	case *Document:
		return FieldDocument, v
	case []interface{}:
		return FieldArray, v
	default:
		return FieldNull, nil
	}
}

// ---------- Sérialisation binaire ----------

// Encode sérialise le document en binaire.
// Format : [nb_fields:uint16] puis pour chaque champ :
//
//	[name_len:uint16][name_bytes][type:byte][value_bytes...]
func (d *Document) Encode() ([]byte, error) {
	buf := make([]byte, 0, 256)
	tmp := make([]byte, 8)

	binary.LittleEndian.PutUint16(tmp, uint16(len(d.Fields)))
	buf = append(buf, tmp[:2]...)

	for _, f := range d.Fields {
		// nom du champ
		nameBytes := []byte(f.Name)
		if len(nameBytes) > math.MaxUint16 {
			return nil, fmt.Errorf("field name too long: %s", f.Name)
		}
		binary.LittleEndian.PutUint16(tmp, uint16(len(nameBytes)))
		buf = append(buf, tmp[:2]...)
		buf = append(buf, nameBytes...)

		// type
		buf = append(buf, byte(f.Type))

		// valeur
		valBytes, err := encodeValue(f.Type, f.Value)
		if err != nil {
			return nil, err
		}
		buf = append(buf, valBytes...)
	}
	return buf, nil
}

// Decode désérialise un document depuis un buffer binaire.
func Decode(data []byte) (*Document, error) {
	if len(data) < 2 {
		return nil, errors.New("document data too short")
	}
	doc := NewDocument()
	offset := 0

	nbFields := int(binary.LittleEndian.Uint16(data[offset:]))
	offset += 2

	for i := 0; i < nbFields; i++ {
		if offset+2 > len(data) {
			return nil, errors.New("unexpected end of document data (name len)")
		}
		nameLen := int(binary.LittleEndian.Uint16(data[offset:]))
		offset += 2

		if offset+nameLen > len(data) {
			return nil, errors.New("unexpected end of document data (name)")
		}
		name := string(data[offset : offset+nameLen])
		offset += nameLen

		if offset >= len(data) {
			return nil, errors.New("unexpected end of document data (type)")
		}
		ftype := FieldType(data[offset])
		offset++

		val, n, err := decodeValue(ftype, data[offset:])
		if err != nil {
			return nil, err
		}
		offset += n
		doc.Fields = append(doc.Fields, Field{Name: name, Type: ftype, Value: val})
	}
	return doc, nil
}

func encodeValue(t FieldType, v interface{}) ([]byte, error) {
	switch t {
	case FieldNull:
		return nil, nil
	case FieldBool:
		if v.(bool) {
			return []byte{1}, nil
		}
		return []byte{0}, nil
	case FieldInt64:
		buf := make([]byte, 8)
		binary.LittleEndian.PutUint64(buf, uint64(v.(int64)))
		return buf, nil
	case FieldFloat64:
		buf := make([]byte, 8)
		binary.LittleEndian.PutUint64(buf, math.Float64bits(v.(float64)))
		return buf, nil
	case FieldString:
		s := v.(string)
		buf := make([]byte, 4+len(s))
		binary.LittleEndian.PutUint32(buf, uint32(len(s)))
		copy(buf[4:], s)
		return buf, nil
	case FieldDocument:
		sub := v.(*Document)
		encoded, err := sub.Encode()
		if err != nil {
			return nil, err
		}
		buf := make([]byte, 4+len(encoded))
		binary.LittleEndian.PutUint32(buf, uint32(len(encoded)))
		copy(buf[4:], encoded)
		return buf, nil
	case FieldArray:
		arr := v.([]interface{})
		// Encode as: [count:uint16] then each element: [type:byte][value_bytes...]
		arrBuf := make([]byte, 0, 64)
		tmp2 := make([]byte, 2)
		binary.LittleEndian.PutUint16(tmp2, uint16(len(arr)))
		arrBuf = append(arrBuf, tmp2...)
		for _, elem := range arr {
			et, ev := inferType(elem)
			arrBuf = append(arrBuf, byte(et))
			eb, err := encodeValue(et, ev)
			if err != nil {
				return nil, err
			}
			arrBuf = append(arrBuf, eb...)
		}
		buf := make([]byte, 4+len(arrBuf))
		binary.LittleEndian.PutUint32(buf, uint32(len(arrBuf)))
		copy(buf[4:], arrBuf)
		return buf, nil
	default:
		return nil, fmt.Errorf("unknown field type: %d", t)
	}
}

func decodeValue(t FieldType, data []byte) (interface{}, int, error) {
	switch t {
	case FieldNull:
		return nil, 0, nil
	case FieldBool:
		if len(data) < 1 {
			return nil, 0, errors.New("not enough data for bool")
		}
		return data[0] != 0, 1, nil
	case FieldInt64:
		if len(data) < 8 {
			return nil, 0, errors.New("not enough data for int64")
		}
		return int64(binary.LittleEndian.Uint64(data)), 8, nil
	case FieldFloat64:
		if len(data) < 8 {
			return nil, 0, errors.New("not enough data for float64")
		}
		return math.Float64frombits(binary.LittleEndian.Uint64(data)), 8, nil
	case FieldString:
		if len(data) < 4 {
			return nil, 0, errors.New("not enough data for string length")
		}
		slen := int(binary.LittleEndian.Uint32(data))
		if len(data) < 4+slen {
			return nil, 0, errors.New("not enough data for string")
		}
		return string(data[4 : 4+slen]), 4 + slen, nil
	case FieldDocument:
		if len(data) < 4 {
			return nil, 0, errors.New("not enough data for embedded document length")
		}
		dlen := int(binary.LittleEndian.Uint32(data))
		if len(data) < 4+dlen {
			return nil, 0, errors.New("not enough data for embedded document")
		}
		sub, err := Decode(data[4 : 4+dlen])
		if err != nil {
			return nil, 0, err
		}
		return sub, 4 + dlen, nil
	case FieldArray:
		if len(data) < 4 {
			return nil, 0, errors.New("not enough data for array length")
		}
		alen := int(binary.LittleEndian.Uint32(data))
		if len(data) < 4+alen {
			return nil, 0, errors.New("not enough data for array")
		}
		arrData := data[4 : 4+alen]
		if len(arrData) < 2 {
			return []interface{}{}, 4 + alen, nil
		}
		count := int(binary.LittleEndian.Uint16(arrData))
		aoff := 2
		arr := make([]interface{}, 0, count)
		for i := 0; i < count; i++ {
			et := FieldType(arrData[aoff])
			aoff++
			ev, n, err := decodeValue(et, arrData[aoff:])
			if err != nil {
				return nil, 0, err
			}
			aoff += n
			arr = append(arr, ev)
		}
		return arr, 4 + alen, nil
	default:
		return nil, 0, fmt.Errorf("unknown field type: %d", t)
	}
}
