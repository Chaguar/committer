/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package sidecar

import (
	"github.com/Chaguar/common/api/applicationpb"
	"github.com/Chaguar/common/api/committerpb"
	"github.com/cockroachdb/errors"
)

type namespaceBlindVersions struct {
	supplied bool
	// A nil version marks an original blind-write key without supplied metadata.
	versions map[string]*uint64
}

// buildCommittedWriteVersions associates results with original writes by namespace and key.
func buildCommittedWriteVersions(namespaces []*applicationpb.TxNamespace,
	blind []*committerpb.NamespaceWriteVersions,
) ([]*committerpb.NamespaceWriteVersions, error) {
	known, err := indexBlindWriteVersions(namespaces, blind)
	if err != nil {
		return nil, err
	}

	results := make([]*committerpb.NamespaceWriteVersions, 0, len(namespaces))
	for _, ns := range namespaces {
		result, buildErr := buildNamespaceWriteVersions(ns, known[ns.NsId].versions)
		if buildErr != nil {
			return nil, buildErr
		}
		if len(result.Writes) > 0 {
			results = append(results, result)
		}
	}

	return results, nil
}

func indexBlindWriteVersions(namespaces []*applicationpb.TxNamespace,
	blind []*committerpb.NamespaceWriteVersions,
) (map[string]*namespaceBlindVersions, error) {
	known, err := indexOriginalBlindWrites(namespaces)
	if err != nil {
		return nil, err
	}

	for _, ns := range blind {
		if ns == nil {
			return nil, errors.New("nil blind-version namespace")
		}
		entry, present := known[ns.NsId]
		if !present {
			return nil, errors.Newf("unknown blind-version namespace %s", ns.NsId)
		}
		if err := entry.addWriteVersions(ns); err != nil {
			return nil, err
		}
	}

	return known, nil
}

func indexOriginalBlindWrites(namespaces []*applicationpb.TxNamespace) (map[string]*namespaceBlindVersions, error) {
	known := make(map[string]*namespaceBlindVersions, len(namespaces))
	for _, ns := range namespaces {
		if ns == nil {
			return nil, errors.New("nil original namespace")
		}
		if _, present := known[ns.NsId]; present {
			return nil, errors.Newf("duplicate original namespace %s", ns.NsId)
		}

		versions := make(map[string]*uint64, len(ns.BlindWrites))
		for _, write := range ns.BlindWrites {
			if write == nil {
				return nil, errors.New("nil original blind-write entry")
			}
			versions[string(write.Key)] = nil
		}
		known[ns.NsId] = &namespaceBlindVersions{versions: versions}
	}
	return known, nil
}

func (entry *namespaceBlindVersions) addWriteVersions(ns *committerpb.NamespaceWriteVersions) error {
	if entry.supplied {
		return errors.Newf("duplicate blind-version namespace %s", ns.NsId)
	}
	entry.supplied = true

	for _, write := range ns.Writes {
		if write == nil {
			return errors.New("nil blind-version entry")
		}
		key := string(write.Key)
		version, present := entry.versions[key]
		if !present {
			return errors.Newf("unknown blind-version key in namespace %s", ns.NsId)
		}
		if version != nil {
			return errors.Newf("duplicate blind-version key in namespace %s", ns.NsId)
		}
		entry.versions[key] = &write.Version
	}
	return nil
}

func buildNamespaceWriteVersions(ns *applicationpb.TxNamespace, blind map[string]*uint64,
) (*committerpb.NamespaceWriteVersions, error) {
	result := &committerpb.NamespaceWriteVersions{NsId: ns.NsId}
	for _, write := range ns.ReadWrites {
		if write == nil {
			return nil, errors.New("nil original read-write entry")
		}
		version := uint64(0)
		if write.Version != nil {
			version = *write.Version + 1
		}
		result.Writes = append(result.Writes, &committerpb.CommittedWriteVersion{Key: write.Key, Version: version})
	}

	for _, write := range ns.BlindWrites {
		if version := blind[string(write.Key)]; version != nil {
			result.Writes = append(result.Writes, &committerpb.CommittedWriteVersion{Key: write.Key, Version: *version})
		}
	}
	return result, nil
}
