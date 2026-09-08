package main

import (
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/hashicorp/hcl/v2/gohcl"
	"github.com/hashicorp/hcl/v2/hclparse"
	"golang.org/x/term"
)

const hclexpConfigFilename = ".hclexp.config"

// hclexpConfigFile separates defaults shared by comparison commands from
// command-specific overrides. Pointer values distinguish an explicit false
// from an omitted setting that should inherit from the preceding layer.
type hclexpConfigFile struct {
	Global *hclexpConfigSection `hcl:"global,block"`
	Diff   *hclexpConfigSection `hcl:"diff,block"`
	Drift  *hclexpConfigSection `hcl:"drift,block"`
	Plan   *hclexpConfigSection `hcl:"plan,block"`
	Web    *hclexpConfigSection `hcl:"web,block"`
}

type hclexpConfigSection struct {
	IgnoreColumnOrder *bool `hcl:"ignore_column_order,optional"`
}

type hclexpConfigResolution struct {
	IgnoreColumnOrder *bool
	Files             []string
}

// configuredIgnoreColumnOrder applies, from lowest to highest precedence:
// built-in flag default, $HOME config, working-directory config, explicit CLI
// flag. Inside each file the subcommand section overrides the global section.
func configuredIgnoreColumnOrder(fs *flag.FlagSet, command string, flagValue bool) (bool, error) {
	workdir, err := os.Getwd()
	if err != nil {
		return false, fmt.Errorf("get working directory: %w", err)
	}
	resolved, err := resolveHCLExpConfig(os.Getenv("HOME"), workdir, command)
	if err != nil {
		return false, err
	}
	renderHCLExpConfigInfoIfTerminal(os.Stderr, resolved.Files, term.IsTerminal(int(os.Stderr.Fd())))
	return effectiveIgnoreColumnOrder(resolved, flagWasSet(fs, "ignore-column-order"), flagValue), nil
}

func resolveHCLExpConfig(home, workdir, command string) (hclexpConfigResolution, error) {
	if !isComparisonCommand(command) {
		return hclexpConfigResolution{}, fmt.Errorf("unsupported config subcommand %q", command)
	}

	var resolved hclexpConfigResolution
	seen := map[string]bool{}
	for _, dir := range []string{home, workdir} {
		if dir == "" {
			continue
		}
		path, err := filepath.Abs(filepath.Join(dir, hclexpConfigFilename))
		if err != nil {
			return hclexpConfigResolution{}, fmt.Errorf("resolve config path in %q: %w", dir, err)
		}
		path = filepath.Clean(path)
		if seen[path] {
			continue
		}
		seen[path] = true
		if _, err := os.Stat(path); err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return hclexpConfigResolution{}, fmt.Errorf("stat %s: %w", path, err)
		}

		parser := hclparse.NewParser()
		file, diagnostics := parser.ParseHCLFile(path)
		if diagnostics.HasErrors() {
			return hclexpConfigResolution{}, fmt.Errorf("parse %s: %s", path, diagnostics)
		}
		var config hclexpConfigFile
		if diagnostics := gohcl.DecodeBody(file.Body, nil, &config); diagnostics.HasErrors() {
			return hclexpConfigResolution{}, fmt.Errorf("parse %s: %s", path, diagnostics)
		}
		resolved.Files = append(resolved.Files, path)
		applyHCLExpConfigSection(&resolved, config.Global)
		applyHCLExpConfigSection(&resolved, config.section(command))
	}
	return resolved, nil
}

func (c hclexpConfigFile) section(command string) *hclexpConfigSection {
	switch command {
	case "diff":
		return c.Diff
	case "drift":
		return c.Drift
	case "plan":
		return c.Plan
	case "web":
		return c.Web
	default:
		return nil
	}
}

func isComparisonCommand(command string) bool {
	switch command {
	case "diff", "drift", "plan", "web":
		return true
	default:
		return false
	}
}

func applyHCLExpConfigSection(resolved *hclexpConfigResolution, section *hclexpConfigSection) {
	if section == nil || section.IgnoreColumnOrder == nil {
		return
	}
	value := *section.IgnoreColumnOrder
	resolved.IgnoreColumnOrder = &value
}

func effectiveIgnoreColumnOrder(resolved hclexpConfigResolution, explicit bool, flagValue bool) bool {
	if explicit {
		return flagValue
	}
	if resolved.IgnoreColumnOrder != nil {
		return *resolved.IgnoreColumnOrder
	}
	return flagValue
}

func renderHCLExpConfigInfo(w io.Writer, files []string) {
	for _, file := range files {
		fmt.Fprintf(w, "hclexp: loaded config %s\n", file)
	}
}

func renderHCLExpConfigInfoIfTerminal(w io.Writer, files []string, terminal bool) {
	if terminal {
		renderHCLExpConfigInfo(w, files)
	}
}
