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
}

type manifestRoleBlock struct {
	Name string             `hcl:"name,label"`
	Envs []manifestEnvBlock `hcl:"env,block"`
}

type manifestEnvBlock struct {
	Name   string   `hcl:"name,label"`
	Layers []string `hcl:"layers"`
}

// manifestRole is a resolved role for one selected environment: a node role and
// the ordered layer dirs whose composition is that role's desired schema.
type manifestRole struct {
	Role   string
	Layers []string
}

// runPlan diffs every role in a manifest against a topology dump and emits one
// globally-ordered operation list with cross-role dependency ordering and role
// provenance. Desired state = the manifest's composed layer stacks; current
// state = the matching node in the dump (matched by hostClusterRole macro,
// replicas collapsed to one representative per role).
func runPlan(args []string) {
	fs := flag.NewFlagSet("hclexp plan", flag.ExitOnError)
	manifestFlag := fs.String("manifest", "", "HCL manifest: role blocks with one env block per environment (desired composition)")
	envFlag := fs.String("env", "", "environment to plan (selects each role's matching env block in the manifest)")
	layerRootFlag := fs.String("layer-root", ".", "root directory the manifest's layer paths resolve under (e.g. a committed snapshot)")
	dumpFlag := fs.String("dump", "", "directory of per-node current-state HCL dumps; nodes are matched to roles by their hostClusterRole macro")
	formatFlag := fs.String("format", "json", "output format: json (default) or text")
	_ = fs.Parse(args)

	if *manifestFlag == "" || *dumpFlag == "" || *envFlag == "" {
		slog.Error("-manifest, -env and -dump are required")
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

	current, err := currentByRole(*dumpFlag)
	if err != nil {
		slog.Error("failed to load dump", "dir", *dumpFlag, "err", err)
		os.Exit(1)
	}

	roleDiffs := make([]hclload.RoleDiff, 0, len(manifest))
	for _, mr := range manifest {
		stack := make([]string, len(mr.Layers))
		for i, l := range mr.Layers {
			stack[i] = filepath.Join(*layerRootFlag, l)
		}
		desired, err := loadSide(strings.Join(stack, ","))
		if err != nil {
			slog.Error("failed to resolve role layers", "role", mr.Role, "layers", stack, "err", err)
			os.Exit(1)
		}
		cur := current[mr.Role]
		if cur == nil {
			cur = &hclload.Schema{} // role absent from the dump: everything is a CREATE
		}
		roleDiffs = append(roleDiffs, hclload.RoleDiff{Role: mr.Role, Desired: desired, Current: cur})
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

// parseManifest decodes the HCL manifest and resolves each role to the layer
// stack for the selected environment. A role with no env block for env is not
// deployed there and is skipped. Duplicate role names, or duplicate env labels
// within a role, are rejected.
func parseManifest(path, env string) ([]manifestRole, error) {
	parser := hclparse.NewParser()
	f, diags := parser.ParseHCLFile(path)
	if diags.HasErrors() {
		return nil, fmt.Errorf("%s", diags)
	}
	var m planManifest
	if diags := gohcl.DecodeBody(f.Body, nil, &m); diags.HasErrors() {
		return nil, fmt.Errorf("%s", diags)
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
	if len(roles) == 0 {
		return nil, fmt.Errorf("no roles deployed in env %q", env)
	}
	return roles, nil
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
		fmt.Fprintf(w, "-- UNSAFE: %s.%s: %s\n", u.Database, u.Object, u.Reason)
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
		fmt.Fprintf(w, "%3d  %-7s %-18s %s.%s  [%s]%s\n",
			op.Order, op.Kind, op.ObjectType, op.Database, op.Object, strings.Join(op.Roles, ","), flag)
	}
}
