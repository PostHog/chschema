package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/hashicorp/hcl/v2/gohcl"
	"github.com/hashicorp/hcl/v2/hclparse"
	hclload "github.com/posthog/chschema/internal/loader/hcl"
)

// planManifest is the HCL manifest: role blocks, each with one env block per
// environment the role is deployed in. Grouping role-first keeps all of a
// cluster's environments in one place — the way an operator (or an LLM) edits
// "the ops cluster".
//
//	role "ops" {
//	  env "prod-us" { layers = ["base", "prod", "env/prod-us"] }
//	  env "prod-eu" { layers = ["base", "prod", "env/prod-eu"] }
//	}
type planManifest struct {
	Roles []manifestRoleBlock `hcl:"role,block"`

	// Clusters is optional cross-cluster metadata used by `validate` and
	// ignored by `plan`/`web`. A ClickHouse cluster is composed of nodes from
	// one or more roles, so each cluster block lists the roles whose
	// compositions (unioned) make up that cluster's schema.
	Clusters []manifestClusterBlock `hcl:"cluster,block"`
}

type manifestRoleBlock struct {
	Name string             `hcl:"name,label"`
	Envs []manifestEnvBlock `hcl:"env,block"`
}

type manifestEnvBlock struct {
	Name   string   `hcl:"name,label"`
	Layers []string `hcl:"layers"`
}

// manifestClusterBlock maps a ClickHouse cluster_name to the roles whose nodes
// compose it (the cluster's schema is their union) and the remote_servers
// aliases that share that composition. A cluster with no composition in the
// manifest (modeled elsewhere) is declared with absent = true instead of roles.
type manifestClusterBlock struct {
	Name    string   `hcl:"name,label"`
	Roles   []string `hcl:"roles,optional"`
	Aliases []string `hcl:"aliases,optional"`
	Absent  bool     `hcl:"absent,optional"`
}

// manifestRole is a resolved role for one selected environment: a node role and
// the ordered layer dirs whose composition is that role's desired schema.
type manifestRole struct {
	Role   string
	Layers []string
}

// runPlan diffs every role in a desired manifest against either a topology
// dump or a previous manifest composition, then emits one globally-ordered
// operation list with cross-role dependency ordering and role provenance.
func runPlan(args []string) {
	fs := flag.NewFlagSet("hclexp plan", flag.ExitOnError)
	manifestFlag := fs.String("manifest", "", "HCL manifest: role blocks with one env block per environment (desired composition)")
	envFlag := fs.String("env", "", "environment to plan (selects each role's matching env block in the manifest)")
	layerRootFlag := fs.String("layer-root", ".", "root directory the manifest's layer paths resolve under (e.g. a committed snapshot)")
	dumpFlag := fs.String("dump", "", "directory of per-node current-state HCL dumps; nodes are matched to roles by their hostClusterRole macro")
	fromManifestFlag := fs.String("from-manifest", "", "previous manifest composition to compare exactly against -manifest (mutually exclusive with -dump)")
	fromLayerRootFlag := fs.String("from-layer-root", "", "root for -from-manifest layer paths (default: directory containing -from-manifest)")
	scopeFlag := fs.String("scope", "all", "dump object scope: all (exact) or desired (ignore live-only objects)")
	formatFlag := fs.String("format", "json", "output format: json (default) or text")
	excludeFlag := fs.String("exclude", "", "HCL exclude config: objects matching its patterns/object_types are dropped from both sides before diffing")
	_ = fs.Parse(args)

	if *manifestFlag == "" || *envFlag == "" {
		slog.Error("-manifest and -env are required")
		os.Exit(2)
	}
	if (*dumpFlag == "") == (*fromManifestFlag == "") {
		slog.Error("exactly one of -dump or -from-manifest is required")
		os.Exit(2)
	}
	if *fromLayerRootFlag != "" && *fromManifestFlag == "" {
		slog.Error("-from-layer-root requires -from-manifest")
		os.Exit(2)
	}
	if *scopeFlag != "all" && *scopeFlag != "desired" {
		slog.Error("invalid -scope (want all or desired)", "scope", *scopeFlag)
		os.Exit(2)
	}
	if *scopeFlag == "desired" && *dumpFlag == "" {
		slog.Error("-scope desired requires -dump; manifest-to-manifest planning is exact")
		os.Exit(2)
	}
	if *formatFlag != "json" && *formatFlag != "text" {
		slog.Error("invalid -format (want json or text)", "format", *formatFlag)
		os.Exit(2)
	}

	manifest, err := parseManifest(*manifestFlag, *envFlag)
	if err != nil {
		slog.Error("failed to parse manifest", "file", *manifestFlag, "env", *envFlag, "err", err)
		os.Exit(1)
	}

	matcher := loadExcludeFlag(*excludeFlag)
	var roleDiffs []hclload.RoleDiff
	if *dumpFlag != "" {
		current, err := currentByRole(*dumpFlag)
		if err != nil {
			slog.Error("failed to load dump", "dir", *dumpFlag, "err", err)
			os.Exit(1)
		}
		roleDiffs, err = roleDiffsFromDump(manifest, *layerRootFlag, current, matcher, *scopeFlag)
		if err != nil {
			slog.Error("failed to build dump plan", "err", err)
			os.Exit(1)
		}
	} else {
		fromManifest, err := parseManifestOptional(*fromManifestFlag, *envFlag)
		if err != nil {
			slog.Error("failed to parse previous manifest", "file", *fromManifestFlag, "env", *envFlag, "err", err)
			os.Exit(1)
		}
		fromRoot := *fromLayerRootFlag
		if fromRoot == "" {
			fromRoot = filepath.Dir(*fromManifestFlag)
		}
		roleDiffs, err = roleDiffsFromManifest(manifest, *layerRootFlag, fromManifest, fromRoot, matcher)
		if err != nil {
			slog.Error("failed to build manifest plan", "err", err)
			os.Exit(1)
		}
	}

	plan := hclload.BuildPlan(roleDiffs)

	if *formatFlag == "json" {
		out, err := json.MarshalIndent(plan, "", "  ")
		if err != nil {
			slog.Error("failed to render plan JSON", "err", err)
			os.Exit(1)
		}
		fmt.Println(string(out))
		return
	}
	renderPlanText(os.Stdout, plan)
}

func roleDiffsFromDump(
	desiredRoles []manifestRole,
	desiredRoot string,
	current map[string]*hclload.Schema,
	matcher *hclload.ExcludeMatcher,
	scope string,
) ([]hclload.RoleDiff, error) {
	out := make([]hclload.RoleDiff, 0, len(desiredRoles))
	for _, role := range desiredRoles {
		desired, err := loadManifestRole(role, desiredRoot)
		if err != nil {
			return nil, err
		}
		cur := current[role.Role]
		if cur == nil {
			cur = &hclload.Schema{}
		}
		hclload.FilterSchema(desired, matcher)
		hclload.FilterSchema(cur, matcher)
		if scope == "desired" {
			cur = hclload.ScopeSchemaToObjects(cur, desired)
		}
		out = append(out, hclload.RoleDiff{Role: role.Role, Desired: desired, Current: cur})
	}
	return out, nil
}

func roleDiffsFromManifest(
	desiredRoles []manifestRole,
	desiredRoot string,
	currentRoles []manifestRole,
	currentRoot string,
	matcher *hclload.ExcludeMatcher,
) ([]hclload.RoleDiff, error) {
	desiredByRole := indexManifestRoles(desiredRoles)
	for _, role := range currentRoles {
		if _, ok := desiredByRole[role.Role]; !ok {
			return nil, fmt.Errorf("previous-only role %q requires explicit deployment decommissioning", role.Role)
		}
	}
	currentByRole := indexManifestRoles(currentRoles)
	out := make([]hclload.RoleDiff, 0, len(desiredRoles))
	for _, role := range desiredRoles {
		desired, err := loadManifestRole(role, desiredRoot)
		if err != nil {
			return nil, err
		}
		current := &hclload.Schema{}
		if from, ok := currentByRole[role.Role]; ok {
			current, err = loadManifestRole(from, currentRoot)
			if err != nil {
				return nil, err
			}
		}
		hclload.FilterSchema(desired, matcher)
		hclload.FilterSchema(current, matcher)
		out = append(out, hclload.RoleDiff{Role: role.Role, Desired: desired, Current: current})
	}
	return out, nil
}

func loadManifestRole(role manifestRole, root string) (*hclload.Schema, error) {
	stack := make([]string, len(role.Layers))
	for i, layer := range role.Layers {
		stack[i] = filepath.Join(root, layer)
	}
	schema, err := loadSide(strings.Join(stack, ","))
	if err != nil {
		return nil, fmt.Errorf("role %q layers %v: %w", role.Role, stack, err)
	}
	return schema, nil
}

func indexManifestRoles(roles []manifestRole) map[string]manifestRole {
	out := make(map[string]manifestRole, len(roles))
	for _, role := range roles {
		out[role.Role] = role
	}
	return out
}

// decodeManifest parses a manifest file into its raw block structure. Every
// manifest consumer (parseManifest, parseManifestClusters, locate) shares it.
func decodeManifest(path string) (*planManifest, error) {
	parser := hclparse.NewParser()
	f, diags := parser.ParseHCLFile(path)
	if diags.HasErrors() {
		return nil, fmt.Errorf("%s", diags)
	}
	var m planManifest
	if diags := gohcl.DecodeBody(f.Body, nil, &m); diags.HasErrors() {
		return nil, fmt.Errorf("%s", diags)
	}
	return &m, nil
}

// parseManifest decodes the HCL manifest and resolves each role to the layer
// stack for the selected environment. A role with no env block for env is not
// deployed there and is skipped. Duplicate role names, or duplicate env labels
// within a role, are rejected.
func parseManifest(path, env string) ([]manifestRole, error) {
	return parseManifestEnv(path, env, true)
}

// parseManifestOptional validates the complete manifest but permits the
// selected environment to have no deployed roles. This is required when a new
// environment or role exists only in the proposed manifest.
func parseManifestOptional(path, env string) ([]manifestRole, error) {
	return parseManifestEnv(path, env, false)
}

func parseManifestEnv(path, env string, requireDeployed bool) ([]manifestRole, error) {
	m, err := decodeManifest(path)
	if err != nil {
		return nil, err
	}
	if len(m.Roles) == 0 {
		return nil, fmt.Errorf("manifest declares no roles")
	}

	var roles []manifestRole
	seenRole := map[string]bool{}
	for _, rb := range m.Roles {
		if seenRole[rb.Name] {
			return nil, fmt.Errorf("duplicate role %q", rb.Name)
		}
		seenRole[rb.Name] = true

		seenEnv := map[string]bool{}
		var layers []string
		found := false
		for _, eb := range rb.Envs {
			if seenEnv[eb.Name] {
				return nil, fmt.Errorf("role %q: duplicate env %q", rb.Name, eb.Name)
			}
			seenEnv[eb.Name] = true
			if eb.Name == env {
				layers = eb.Layers
				found = true
			}
		}
		if !found {
			continue // role not deployed in this env
		}
		if len(layers) == 0 {
			return nil, fmt.Errorf("role %q env %q: layers is empty", rb.Name, env)
		}
		roles = append(roles, manifestRole{Role: rb.Name, Layers: layers})
	}
	if requireDeployed && len(roles) == 0 {
		return nil, fmt.Errorf("no roles deployed in env %q", env)
	}
	return roles, nil
}

// parseManifestClusters decodes the optional cluster blocks from the manifest.
// Each names a ClickHouse cluster_name and the roles whose compositions union
// into its schema (plus remote_servers aliases). Duplicate cluster names, or a
// cluster with no roles, are rejected. Returns an empty slice when the manifest
// declares no clusters.
func parseManifestClusters(path string) ([]manifestClusterBlock, error) {
	m, err := decodeManifest(path)
	if err != nil {
		return nil, err
	}
	declaredRoles := make(map[string]bool, len(m.Roles))
	for _, rb := range m.Roles {
		declaredRoles[rb.Name] = true
	}
	seen := map[string]bool{}
	for _, c := range m.Clusters {
		if seen[c.Name] {
			return nil, fmt.Errorf("duplicate cluster %q", c.Name)
		}
		seen[c.Name] = true
		// A cluster is either composed from roles or explicitly absent (no
		// composition in this manifest), never both nor neither.
		if c.Absent {
			if len(c.Roles) > 0 {
				return nil, fmt.Errorf("cluster %q: absent and roles are mutually exclusive", c.Name)
			}
			continue
		}
		if len(c.Roles) == 0 {
			return nil, fmt.Errorf("cluster %q: set roles or absent = true", c.Name)
		}
		for _, role := range c.Roles {
			if !declaredRoles[role] {
				return nil, fmt.Errorf("cluster %q: unknown role %q (no such role block in the manifest)", c.Name, role)
			}
		}
	}
	return m.Clusters, nil
}

// currentByRole loads every per-node dump in dir and returns one representative
// schema per role, keyed by the node's hostClusterRole macro (falling back to
// the role parsed from the filename). Replicas/shards of a role collapse to the
// lexically-first node, so an N-replica role yields one current schema.
func currentByRole(dir string) (map[string]*hclload.Schema, error) {
	nodes, err := loadDriftNodes(dir, "*")
	if err != nil {
		return nil, err
	}
	sort.Slice(nodes, func(i, j int) bool { return nodes[i].Name < nodes[j].Name })

	byRole := map[string]*hclload.Schema{}
	for _, n := range nodes {
		role := n.Macros["hostClusterRole"]
		if role == "" {
			role = n.Role
		}
		if role == "" {
			continue
		}
		if _, ok := byRole[role]; !ok {
			byRole[role] = n.Schema
		}
	}
	return byRole, nil
}

// renderPlanText prints a human-readable, globally-ordered plan.
func renderPlanText(w *os.File, plan hclload.PlanResult) {
	for _, u := range plan.Unsafe {
		fmt.Fprintf(w, "-- UNSAFE: %s: %s\n", qualifiedName(u.Database, u.Object), u.Reason)
	}
	if len(plan.Operations) == 0 {
		fmt.Fprintln(w, "no changes")
		return
	}
	for _, op := range plan.Operations {
		flag := ""
		if op.Unsafe {
			flag = " (UNSAFE)"
		}
		if op.Manual {
			flag += " (MANUAL)"
		}
		fmt.Fprintf(w, "%3d  %-7s %-18s %s.%s  [%s]%s\n",
			op.Order, op.Kind, op.ObjectType, op.Database, op.Object, strings.Join(op.Roles, ","), flag)
	}
}
