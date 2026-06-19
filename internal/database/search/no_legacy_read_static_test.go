package search

import (
	"bufio"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

type legacyReadFinding struct {
	path    string
	line    int
	pattern string
}

var noLegacyReadForbiddenPatterns = []string{
	".TorrentFiles(",
	"HydrateTorrentContentTorrentWithFiles(",
	"Torrent.Files.RelationField",
}

func TestNoLegacyReadStaticAllowlist(t *testing.T) {
	repoRoot, err := findRepoRoot()
	if err != nil {
		t.Fatal(err)
	}

	var findings []legacyReadFinding

	err = filepath.WalkDir(repoRoot, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			if shouldSkipNoLegacyReadDir(entry.Name()) {
				return filepath.SkipDir
			}
			return nil
		}
		if shouldSkipNoLegacyReadFile(path) {
			return nil
		}

		file, readErr := os.Open(path)
		if readErr != nil {
			return readErr
		}

		rel, relErr := filepath.Rel(repoRoot, path)
		if relErr != nil {
			_ = file.Close()
			return relErr
		}
		rel = filepath.ToSlash(rel)

		scanner := bufio.NewScanner(file)
		for lineNo := 1; scanner.Scan(); lineNo++ {
			line := scanner.Text()
			for _, pattern := range noLegacyReadForbiddenPatterns {
				if strings.Contains(line, pattern) && !noLegacyReadAllowed(rel, line, pattern) {
					findings = append(findings, legacyReadFinding{
						path:    rel,
						line:    lineNo,
						pattern: pattern,
					})
				}
			}
		}
		if scanErr := scanner.Err(); scanErr != nil {
			_ = file.Close()
			return scanErr
		}

		return file.Close()
	})
	if err != nil {
		t.Fatal(err)
	}

	if len(findings) > 0 {
		var msg strings.Builder
		msg.WriteString("production legacy torrent_files read references are not allowlisted:\n")
		for _, finding := range findings {
			msg.WriteString("  - ")
			msg.WriteString(finding.path)
			msg.WriteString(":")
			msg.WriteString(testLineNumber(finding.line))
			msg.WriteString(" contains ")
			msg.WriteString(finding.pattern)
			msg.WriteString("\n")
		}
		msg.WriteString("Add a fail-closed DropCompatibleReads guard or classify the reference as pre-DROP tooling before allowlisting it.")
		t.Fatal(msg.String())
	}
}

func findRepoRoot() (string, error) {
	dir, err := os.Getwd()
	if err != nil {
		return "", err
	}

	for {
		if _, statErr := os.Stat(filepath.Join(dir, "go.mod")); statErr == nil {
			return dir, nil
		}

		parent := filepath.Dir(dir)
		if parent == dir {
			return "", os.ErrNotExist
		}
		dir = parent
	}
}

func shouldSkipNoLegacyReadDir(name string) bool {
	switch name {
	case ".git", ".jj", "bitmagnet-rs", "node_modules", "webui":
		return true
	default:
		return false
	}
}

func shouldSkipNoLegacyReadFile(path string) bool {
	if filepath.Ext(path) != ".go" {
		return true
	}
	if strings.HasSuffix(path, "_test.go") || strings.HasSuffix(path, ".gen.go") {
		return true
	}
	if strings.HasSuffix(path, filepath.Join("internal", "gql", "gql.gen.go")) {
		return true
	}

	return false
}

func noLegacyReadAllowed(path string, line string, pattern string) bool {
	switch pattern {
	case ".TorrentFiles(":
		return path == "internal/gql/gqlmodel/torrent_files.go"
	case "HydrateTorrentContentTorrentWithFiles(":
		return path == "internal/database/search/hydrator_torrent_content_torrent.go" &&
			strings.HasPrefix(strings.TrimSpace(line), "func HydrateTorrentContentTorrentWithFiles(")
	case "Torrent.Files.RelationField":
		return path == "internal/database/search/hydrator_torrent_content_torrent.go"
	default:
		return false
	}
}

func testLineNumber(line int) string {
	return strconv.Itoa(line)
}
