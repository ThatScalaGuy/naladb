package store

import (
	"github.com/thatscalaguy/naladb/internal/hlc"
	"github.com/thatscalaguy/naladb/internal/index"
)

// VersionExport represents a single version for snapshot export/import.
type VersionExport struct {
	TS        hlc.HLC `json:"ts"`
	Value     []byte  `json:"value"`
	Tombstone bool    `json:"tombstone"`
	BlobRef   bool    `json:"blob_ref"`
}

// ExportVersions returns the complete version log for all keys.
// This is used by the RAFT snapshot mechanism to capture the full store state.
func (s *Store) ExportVersions() map[string][]VersionExport {
	s.vmu.RLock()
	defer s.vmu.RUnlock()

	result := make(map[string][]VersionExport, len(s.vlog))
	for key, versions := range s.vlog {
		exported := make([]VersionExport, len(versions))
		for i, v := range versions {
			exported[i] = VersionExport{
				TS:        v.ts,
				Value:     v.value,
				Tombstone: v.tombstone,
				BlobRef:   v.blobRef,
			}
		}
		result[key] = exported
	}
	return result
}

// RestoreVersions replaces the entire store state with the provided versions.
// This is used by the RAFT snapshot restore mechanism.
func (s *Store) RestoreVersions(data map[string][]VersionExport) {
	s.vmu.Lock()
	defer s.vmu.Unlock()

	// Clear existing state.
	s.vlog = make(map[string][]version, len(data))
	s.idx = index.New()

	for key, exported := range data {
		versions := make([]version, len(exported))
		for i, e := range exported {
			versions[i] = version{
				ts:        e.TS,
				value:     e.Value,
				tombstone: e.Tombstone,
				blobRef:   e.BlobRef,
			}
		}
		s.vlog[key] = versions

		// Update index with the latest version.
		if len(versions) > 0 {
			latest := versions[len(versions)-1]
			if latest.tombstone {
				s.idx.Delete(key, latest.ts)
			} else {
				s.idx.Put(key, latest.ts, latest.value, false)
				if latest.blobRef {
					if entry, ok := s.idx.GetEntry(key); ok {
						entry.BlobRef = true
						s.idx.PutEntry(key, entry)
					}
				}
			}
		}
	}
}
