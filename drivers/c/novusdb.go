package main

/*
#include <stdlib.h>
*/
import "C"
import (
	"encoding/json"
	"sync"
	"unsafe"

	"github.com/Felmond13/novusdb/api"
	"github.com/Felmond13/novusdb/storage"
)

// handleStore stocke les connexions ouvertes par handle numérique.
var (
	handleMu   sync.Mutex
	handles          = make(map[int64]*api.DB)
	nextHandle int64 = 1
	lastError        = make(map[int64]string)
)

// NovusDB_open ouvre une base NovusDB.
// Retourne un handle > 0 en cas de succès, 0 en cas d'erreur.
//
//export NovusDB_open
func NovusDB_open(path *C.char) C.longlong {
	goPath := C.GoString(path)
	db, err := api.Open(goPath)
	if err != nil {
		return 0
	}
	handleMu.Lock()
	h := nextHandle
	nextHandle++
	handles[h] = db
	handleMu.Unlock()
	return C.longlong(h)
}

// NovusDB_close ferme une connexion.
// Retourne 0 en cas de succès, -1 en cas d'erreur.
//
//export NovusDB_close
func NovusDB_close(handle C.longlong) C.int {
	h := int64(handle)
	handleMu.Lock()
	db, ok := handles[h]
	if ok {
		delete(handles, h)
		delete(lastError, h)
	}
	handleMu.Unlock()
	if !ok {
		return -1
	}
	db.Close()
	return 0
}

// NovusDB_exec exécute une requête SQL.
// Retourne un JSON alloué en C (à libérer avec NovusDB_free).
// Format : {"docs":[...], "rows_affected":N, "last_insert_id":N}
// En cas d'erreur : {"error":"..."}
//
//export NovusDB_exec
func NovusDB_exec(handle C.longlong, sql *C.char) *C.char {
	h := int64(handle)
	handleMu.Lock()
	db, ok := handles[h]
	handleMu.Unlock()
	if !ok {
		return C.CString(`{"error":"invalid handle"}`)
	}

	goSQL := C.GoString(sql)
	result, err := db.Exec(goSQL)
	if err != nil {
		handleMu.Lock()
		lastError[h] = err.Error()
		handleMu.Unlock()
		errJSON, _ := json.Marshal(map[string]string{"error": err.Error()})
		return C.CString(string(errJSON))
	}

	resp := map[string]interface{}{
		"rows_affected":  result.RowsAffected,
		"last_insert_id": result.LastInsertID,
	}
	if result.Docs != nil {
		docs := make([]map[string]interface{}, len(result.Docs))
		for i, rd := range result.Docs {
			docs[i] = docToMap(rd.Doc)
		}
		resp["docs"] = docs
	}

	out, _ := json.Marshal(resp)
	return C.CString(string(out))
}

// NovusDB_insert_json insère un document JSON brut dans une collection.
// Retourne l'ID du document inséré, ou -1 en cas d'erreur.
//
//export NovusDB_insert_json
func NovusDB_insert_json(handle C.longlong, collection *C.char, jsonStr *C.char) C.longlong {
	h := int64(handle)
	handleMu.Lock()
	db, ok := handles[h]
	handleMu.Unlock()
	if !ok {
		return -1
	}

	goCol := C.GoString(collection)
	goJSON := C.GoString(jsonStr)
	id, err := db.InsertJSON(goCol, goJSON)
	if err != nil {
		handleMu.Lock()
		lastError[h] = err.Error()
		handleMu.Unlock()
		return -1
	}
	return C.longlong(id)
}

// NovusDB_collections retourne la liste des collections en JSON.
// À libérer avec NovusDB_free.
//
//export NovusDB_collections
func NovusDB_collections(handle C.longlong) *C.char {
	h := int64(handle)
	handleMu.Lock()
	db, ok := handles[h]
	handleMu.Unlock()
	if !ok {
		return C.CString(`[]`)
	}
	out, _ := json.Marshal(db.Collections())
	return C.CString(string(out))
}

// NovusDB_error retourne la dernière erreur pour un handle.
// À libérer avec NovusDB_free.
//
//export NovusDB_error
func NovusDB_error(handle C.longlong) *C.char {
	h := int64(handle)
	handleMu.Lock()
	e := lastError[h]
	handleMu.Unlock()
	return C.CString(e)
}

// NovusDB_dump retourne le dump SQL complet.
// À libérer avec NovusDB_free.
//
//export NovusDB_dump
func NovusDB_dump(handle C.longlong) *C.char {
	h := int64(handle)
	handleMu.Lock()
	db, ok := handles[h]
	handleMu.Unlock()
	if !ok {
		return C.CString("")
	}
	return C.CString(db.Dump())
}

// NovusDB_free libère une chaîne allouée par les fonctions NovusDB_*.
//
//export NovusDB_free
func NovusDB_free(ptr *C.char) {
	C.free(unsafe.Pointer(ptr))
}

// docToMap convertit un Document en map récursivement.
func docToMap(doc *storage.Document) map[string]interface{} {
	m := make(map[string]interface{})
	for _, f := range doc.Fields {
		switch v := f.Value.(type) {
		case *storage.Document:
			m[f.Name] = docToMap(v)
		default:
			m[f.Name] = f.Value
		}
	}
	return m
}

func main() {}
