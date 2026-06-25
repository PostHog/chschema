package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"

	hclload "github.com/posthog/chschema/internal/loader/hcl"
)

// manifestRole is one line of a plan manifest: a node role and the ordered
// layer dirs whose composition is that role's desired schema.
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
	manifestFlag := fs.String("manifest", "", "manifest file: lines of '<role> <layer> [<layer>...]' (desired composition per role)")
	layerRootFlag := fs.String("layer-root", ".", "root directory the manifest's layer paths resolve under (e.g. a committed snapshot)")
	dumpFlag := fs.String("dump", "", "directory of per-node current-state HCL dumps; nodes are matched to roles by their hostClusterRole macro")
	formatFlag := fs.String("format", "json", "output format: json (default) or text")
	_ = fs.Parse(args)

	if *manifestFlag == "" || *dumpFlag == "" {
		slog.Error("both -manifest and -dump are required")
		os.Exit(2)
	}
	if *formatFlag != "json" && *formatFlag != "text" {
		slog.Error("invalid -format (want json or text)", "format", *formatFlag)
		os.Exit(2)
	}

	manifest, err := parseManifest(*manifestFlag)
	if err != nil {
		slog.Error("failed to parse manifest", "file", *manifestFlag, "err", err)
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

// parseManifest reads a manifest file. Blank lines and lines beginning with '#'
// are ignored; every other line is '<role> <layer> [<layer>...]'.
func parseManifest(path string) ([]manifestRole, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var roles []manifestRole
	seen := map[string]bool{}
	sc := bufio.NewScanner(f)
	for line := 1; sc.Scan(); line++ {
		fields := strings.Fields(sc.Text())
		if len(fields) == 0 || strings.HasPrefix(fields[0], "#") {
			continue
		}
		if len(fields) < 2 {
			return nil, fmt.Errorf("line %d: want '<role> <layer> [<layer>...]', got %q", line, sc.Text())
		}
		role := fields[0]
		if seen[role] {
			return nil, fmt.Errorf("line %d: duplicate role %q", line, role)
		}
		seen[role] = true
		roles = append(roles, manifestRole{Role: role, Layers: fields[1:]})
	}
	if err := sc.Err(); err != nil {
		return nil, err
	}
	if len(roles) == 0 {
		return nil, fmt.Errorf("manifest is empty")
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
