package workspace

import (
	"fmt"
	"strconv"

	"github.com/philcantcode/go-world-management-layer/internal/domain"
)

// DomainChanges converts the authoritative manifest diff into the canonical
// domain representation used by workspace and target evidence bundles.
func DomainChanges(before, after Manifest) ([]domain.ChangeEntry, error) {
	changes, err := Diff(before, after)
	if err != nil {
		return nil, err
	}
	result := make([]domain.ChangeEntry, 0, len(changes))
	for index, change := range changes {
		kind, err := domainChangeKind(change.Kind)
		if err != nil {
			return nil, fmt.Errorf("change %d: %w", index, err)
		}
		beforeDigest, err := domainEntryDigest(change.Before)
		if err != nil {
			return nil, fmt.Errorf("change %d before digest: %w", index, err)
		}
		afterDigest, err := domainEntryDigest(change.After)
		if err != nil {
			return nil, fmt.Errorf("change %d after digest: %w", index, err)
		}
		entry, err := domain.NewChangeEntry(domain.ChangeEntrySpec{
			Kind: kind, Path: change.Path, PreviousPath: change.PreviousPath,
			BeforeDigest: beforeDigest, AfterDigest: afterDigest,
			Metadata: domainChangeMetadata(change.Before, change.After),
		})
		if err != nil {
			return nil, fmt.Errorf("change %d: %w", index, err)
		}
		result = append(result, entry)
	}
	return result, nil
}

func domainChangeKind(kind ChangeKind) (domain.ChangeKind, error) {
	switch kind {
	case ChangeAdded:
		return domain.ChangeAdded, nil
	case ChangeModified:
		return domain.ChangeModified, nil
	case ChangeDeleted:
		return domain.ChangeDeleted, nil
	case ChangeRenamed:
		return domain.ChangeRenamed, nil
	case ChangeMetadata:
		return domain.ChangeMetadataOnly, nil
	case ChangeOpaque:
		return domain.ChangeOpaqueDirectory, nil
	default:
		return "", fmt.Errorf("unsupported change kind %q", kind)
	}
}

func domainEntryDigest(entry *Entry) (domain.Digest, error) {
	if entry == nil {
		return domain.Digest{}, nil
	}
	return domain.ParseDigest(entry.Digest)
}

func domainChangeMetadata(before, after *Entry) map[string]string {
	metadata := make(map[string]string, 6)
	appendDomainEntryMetadata(metadata, "before", before)
	appendDomainEntryMetadata(metadata, "after", after)
	return metadata
}

func appendDomainEntryMetadata(metadata map[string]string, prefix string, entry *Entry) {
	if entry == nil {
		return
	}
	metadata[prefix+"_size_bytes"] = strconv.FormatInt(entry.Size, 10)
	metadata[prefix+"_mode"] = fmt.Sprintf("%#o", entry.Mode)
	metadata[prefix+"_modified_ns"] = strconv.FormatInt(entry.ModifiedNS, 10)
}
