package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"regexp"
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

// defaultOutName is the -out-name template preserving the flat
// <env>-<role>.hcl layout load has always written.
const defaultOutName = "{env}-{role}"

// loadFlags carries every `load` flag the pure validation needs. configSet
// says whether -config was given explicitly (it has a non-empty default).
type loadFlags struct {
	manifest, env, role, format, layers, outName string
	exclude, excludeObjects, only                string
	configSet                                    bool
}

// loadFlagsError reports the usage error in a `load` invocation, if any. It is
// pure so the exit-2 paths are testable without a subprocess.
func loadFlagsError(f loadFlags) error {
	if (f.manifest == "") != (f.env == "") {
		return fmt.Errorf("-manifest and -env must be used together")
	}
	if f.format != "hcl" && f.format != "json" {
		return fmt.Errorf("invalid -format %q (want hcl or json)", f.format)
	}
	// The JSON document is the layer stacks, not a schema — there are no
	// objects for a filter to act on.
	if f.format == "json" && (f.exclude != "" || f.excludeObjects != "" || f.only != "") {
		return fmt.Errorf("-exclude/-exclude-objects/-only apply to the emitted schema, not -format json")
	}
	if err := validGlobList("-exclude-objects", f.excludeObjects); err != nil {
		return err
	}
	if err := validGlobList("-only", f.only); err != nil {
		return err
	}
	if f.manifest == "" {
		if f.role != "" {
			return fmt.Errorf("-role requires -manifest")
		}
		if f.outName != defaultOutName {
			return fmt.Errorf("-out-name requires -manifest (it names composed role files)")
		}
		// Without a manifest the layer stack is the flag value itself, so
		// there is nothing for -format json to resolve.
		if f.format == "json" {
			return fmt.Errorf("-format json requires -manifest")
		}
		return nil
	}
	if f.layers != "" || f.configSet {
		return fmt.Errorf("-manifest is mutually exclusive with -layer and -config")
	}
	if f.outName != defaultOutName {
		if f.format == "json" {
			return fmt.Errorf("-out-name applies to hcl output, not -format json")
		}
		if _, err := renderOutName(f.outName, "env", "role"); err != nil {
			return err
		}
	}
	return nil
}

// validGlobList checks every comma-separated pattern in a flag value.
func validGlobList(flagName, list string) error {
	for _, p := range splitList(list) {
		if _, err := filepath.Match(p, ""); err != nil {
			return fmt.Errorf("invalid %s pattern %q: %w", flagName, p, err)
		}
	}
	return nil
}

// loadFilters is the assembled object filter applied to every schema `load`
// emits: the -exclude config, the ad-hoc -exclude-objects globs, and the
// -only selector.
type loadFilters struct {
	exclude      *hclload.ExcludeMatcher
	excludeGlobs []string
	onlyGlobs    []string
}

// apply drops the excluded objects and then keeps only the -only matches. An
// object survives iff it matches -only (when given) and neither exclusion
// source. Databases survive emptied (a split layer still needs the database{}
// wrapper and its cluster default) and node{} blocks are never touched.
func (lf loadFilters) apply(s *hclload.Schema) {
	hclload.FilterSchema(s, lf.exclude)
	if len(lf.excludeGlobs) > 0 {
		hclload.FilterSchema(s, hclload.NewExcludeMatcher(lf.excludeGlobs...))
	}
	if len(lf.onlyGlobs) > 0 {
		hclload.SelectSchema(s, hclload.NewExcludeMatcher(lf.onlyGlobs...))
	}
}

// outNamePlaceholderRe matches a {placeholder} left over after expansion, so
// a typo like {rolle} fails loudly instead of producing literal braces in
// filenames.
var outNamePlaceholderRe = regexp.MustCompile(`\{[^{}/]*\}`)

// renderOutName expands the {env} and {role} placeholders in an -out-name
// template. Any other placeholder is an error.
func renderOutName(template, env, role string) (string, error) {
	s := strings.ReplaceAll(template, "{env}", env)
	s = strings.ReplaceAll(s, "{role}", role)
	if m := outNamePlaceholderRe.FindString(s); m != "" {
		return "", fmt.Errorf("unknown placeholder %s in -out-name %q (want {env} or {role})", m, template)
	}
	return s, nil
}

// outNamePath places a rendered -out-name (plus the .hcl extension) under the
// -out directory, rejecting names that escape it.
func outNamePath(dir, rendered string) (string, error) {
	if filepath.IsAbs(rendered) {
		return "", fmt.Errorf("-out-name %q must be relative to -out", rendered)
	}
	path := filepath.Join(dir, rendered+".hcl")
	rel, err := filepath.Rel(dir, path)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("-out-name %q escapes the -out directory", rendered)
	}
	return path, nil
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
// non-empty), applies the object filters, and writes them out. In hcl format a
// single role goes to a file or stdout and multiple roles go to the -out
// directory, each named by the -out-name template (default <env>-<role>.hcl);
// in json format the resolved layer stacks go to stdout or -out.
func runLoadManifest(manifestPath, env, role, layerRoot, format, out, outName string, filters loadFilters) {
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

	// The JSON document is the stacks themselves, so it resolves structurally
	// — the layers need not exist yet ("what should I fetch?" on a fresh clone).
	if format == "json" {
		if err := writeLoadJSON(out, env, resolveManifestStacks(roles, layerRoot)); err != nil {
			slog.Error("failed to write JSON", "err", err)
			os.Exit(1)
		}
		return
	}

	composed, err := composeManifestRoles(roles, layerRoot)
	if err != nil {
		slog.Error("failed to compose manifest roles", "file", manifestPath, "env", env, "err", err)
		os.Exit(1)
	}
	for i := range composed {
		filters.apply(composed[i].Schema)
	}

	if err := checkOutTarget(out, len(composed)); err != nil {
		slog.Error("invalid output target", "env", env, "err", err)
		os.Exit(2)
	}
	// The template names files inside a directory; a single role headed for
	// stdout or a plain file has nothing for it to name.
	if outName != defaultOutName && !isDir(out) {
		slog.Error("invalid output target", "env", env, "err",
			fmt.Errorf("-out-name requires -out to be an existing directory"))
		os.Exit(2)
	}

	if err := writeComposedRoles(out, env, outName, composed); err != nil {
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
// its outName-templated path inside it; several roles always go to the -out
// directory. Template subdirectories (e.g. '{env}/{role}') are created; two
// roles rendering to the same path is an error rather than a silent overwrite.
func writeComposedRoles(out, env, outName string, composed []composedRole) error {
	if len(composed) == 1 && stdoutTarget(out) {
		return hclload.Write(os.Stdout, composed[0].Schema)
	}
	if len(composed) == 1 && !isDir(out) {
		return writeSchemaFile(out, composed[0].Schema)
	}
	pathRole := map[string]string{}
	for _, c := range composed {
		rendered, err := renderOutName(outName, env, c.Role)
		if err != nil {
			return err
		}
		path, err := outNamePath(out, rendered)
		if err != nil {
			return err
		}
		if prev, ok := pathRole[path]; ok {
			return fmt.Errorf("roles %q and %q both render -out-name %q to %s (add {role} to the template)", prev, c.Role, outName, path)
		}
		pathRole[path] = c.Role
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			return fmt.Errorf("role %q: %w", c.Role, err)
		}
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
