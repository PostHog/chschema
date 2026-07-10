package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"

	hclload "github.com/posthog/chschema/internal/loader/hcl"
)

// loadRoleJSON is one role's resolved layer stack in the `load -format json`
// document. Layers is what the manifest declares; ResolvedLayers is those
// paths joined under -layer-root, i.e. the dirs handed to the loader.
type loadRoleJSON struct {
	Role           string   `json:"role"`
	Layers         []string `json:"layers"`
	ResolvedLayers []string `json:"resolved_layers"`
}

// loadJSON is the document emitted by `hclexp load -manifest -format json`.
// It lets callers that genuinely need a role's layer stack ask for it instead
// of parsing manifest.hcl themselves.
type loadJSON struct {
	Env   string         `json:"env"`
	Roles []loadRoleJSON `json:"roles"`
}

// loadFlagsError reports the usage error in a `load` invocation, if any. It is
// pure so the exit-2 paths are testable without a subprocess. configSet says
// whether -config was given explicitly (it has a non-empty default).
func loadFlagsError(manifest, env, role, format, layers string, configSet bool) error {
	if (manifest == "") != (env == "") {
		return fmt.Errorf("-manifest and -env must be used together")
	}
	if format != "hcl" && format != "json" {
		return fmt.Errorf("invalid -format %q (want hcl or json)", format)
	}
	if manifest == "" {
		if role != "" {
			return fmt.Errorf("-role requires -manifest")
		}
		// Without a manifest the layer stack is the flag value itself, so
		// there is nothing for -format json to resolve.
		if format == "json" {
			return fmt.Errorf("-format json requires -manifest")
		}
		return nil
	}
	if layers != "" || configSet {
		return fmt.Errorf("-manifest is mutually exclusive with -layer and -config")
	}
	return nil
}

// checkOutTarget reports whether out can receive nRoles composed schemas.
// Several composed roles concatenated onto one stream would repeat their
// database blocks, so the result would not load back as a single node schema;
// they need a directory instead.
func checkOutTarget(out string, nRoles int) error {
	if nRoles <= 1 {
		return nil
	}
	if stdoutTarget(out) {
		return fmt.Errorf("-out (a directory) is required when composing %d roles", nRoles)
	}
	if !isDir(out) {
		return fmt.Errorf("-out %q must be an existing directory when composing %d roles", out, nRoles)
	}
	return nil
}

// runLoadManifest composes the manifest's roles for env (filtered to role when
// non-empty) and writes them out. In hcl format a single role goes to a file or
// stdout and multiple roles go to the -out directory as one <env>-<role>.hcl
// each; in json format the resolved layer stacks go to stdout or -out.
func runLoadManifest(manifestPath, env, role, layerRoot, format, out string) {
	roles, err := parseManifest(manifestPath, env)
	if err != nil {
		slog.Error("failed to parse manifest", "file", manifestPath, "env", env, "err", err)
		os.Exit(1)
	}
	roles, err = filterManifestRoles(roles, role, env)
	if err != nil {
		slog.Error("failed to select role", "file", manifestPath, "env", env, "err", err)
		os.Exit(2)
	}

	composed, err := composeManifestRoles(roles, layerRoot)
	if err != nil {
		slog.Error("failed to compose manifest roles", "file", manifestPath, "env", env, "err", err)
		os.Exit(1)
	}

	if format == "json" {
		if err := writeLoadJSON(out, env, composed); err != nil {
			slog.Error("failed to write JSON", "err", err)
			os.Exit(1)
		}
		return
	}

	if err := checkOutTarget(out, len(composed)); err != nil {
		slog.Error("invalid output target", "env", env, "err", err)
		os.Exit(2)
	}

	if err := writeComposedRoles(out, env, composed); err != nil {
		slog.Error("failed to write composed schema", "err", err)
		os.Exit(1)
	}
	slog.Info("composed manifest roles", "env", env, "roles", len(composed))
}

// errUnknownRole marks a -role that names no role deployed in the env. It is a
// usage error, so every command that filters by role exits 2 on it.
var errUnknownRole = errors.New("unknown role")

// filterManifestRoles narrows roles to the named one. An empty name keeps every
// role. An unknown name is an errUnknownRole naming the roles deployed in env.
func filterManifestRoles(roles []manifestRole, role, env string) ([]manifestRole, error) {
	if role == "" {
		return roles, nil
	}
	for _, r := range roles {
		if r.Role == role {
			return []manifestRole{r}, nil
		}
	}
	names := make([]string, 0, len(roles))
	for _, r := range roles {
		names = append(names, r.Role)
	}
	sort.Strings(names)
	return nil, fmt.Errorf("%w %q in env %q (deployed roles: %s)", errUnknownRole, role, env, strings.Join(names, ", "))
}

// writeLoadJSON renders the resolved layer stacks to out ("" or "-" is stdout).
func writeLoadJSON(out, env string, composed []composedRole) error {
	doc := loadJSON{Env: env, Roles: make([]loadRoleJSON, 0, len(composed))}
	for _, c := range composed {
		doc.Roles = append(doc.Roles, loadRoleJSON{
			Role:           c.Role,
			Layers:         c.Layers,
			ResolvedLayers: c.Resolved,
		})
	}
	buf, err := json.MarshalIndent(doc, "", "  ")
	if err != nil {
		return err
	}
	buf = append(buf, '\n')

	if stdoutTarget(out) {
		_, err := os.Stdout.Write(buf)
		return err
	}
	return os.WriteFile(out, buf, 0o644)
}

// writeComposedRoles writes each composed role as canonical HCL. A single role
// goes to stdout, to the -out file, or (when -out is an existing directory) to
// <env>-<role>.hcl inside it; several roles always go to the -out directory.
func writeComposedRoles(out, env string, composed []composedRole) error {
	if len(composed) == 1 && stdoutTarget(out) {
		return hclload.Write(os.Stdout, composed[0].Schema)
	}
	if len(composed) == 1 && !isDir(out) {
		return writeSchemaFile(out, composed[0].Schema)
	}
	for _, c := range composed {
		path := filepath.Join(out, fmt.Sprintf("%s-%s.hcl", env, c.Role))
		if err := writeSchemaFile(path, c.Schema); err != nil {
			return fmt.Errorf("role %q: %w", c.Role, err)
		}
	}
	return nil
}

// writeSchemaFile writes one resolved schema to path as canonical HCL.
func writeSchemaFile(path string, schema *hclload.Schema) error {
	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("open %q: %w", path, err)
	}
	defer f.Close()
	return hclload.Write(f, schema)
}

// isDir reports whether path exists and is a directory.
func isDir(path string) bool {
	fi, err := os.Stat(path)
	return err == nil && fi.IsDir()
}
