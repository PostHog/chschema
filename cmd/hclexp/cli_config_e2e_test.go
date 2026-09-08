package main

import (
	"bytes"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	hclload "github.com/posthog/chschema/internal/loader/hcl"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHCLExpConfigCLIProcess(t *testing.T) {
	if os.Getenv("HCLEXP_CONFIG_HELPER") != "1" {
		return
	}
	for i, arg := range os.Args {
		if arg != "--" || i+1 >= len(os.Args) {
			continue
		}
		command, commandArgs := os.Args[i+1], os.Args[i+2:]
		switch command {
		case "diff":
			runDiff(commandArgs)
		case "drift":
			runDrift(commandArgs)
		case "plan":
			runPlan(commandArgs)
		case "web":
			runWeb(commandArgs)
		default:
			t.Fatalf("unsupported helper command %q", command)
		}
		os.Exit(0)
	}
	t.Fatal("missing CLI arguments")
}

type hclexpConfigE2EFixture struct {
	home, workdir, left, right, dump, manifest, desiredRoot string
}

func newHCLExpConfigE2EFixture(t *testing.T) hclexpConfigE2EFixture {
	t.Helper()
	root := t.TempDir()
	fixture := hclexpConfigE2EFixture{
		home:        filepath.Join(root, "home"),
		workdir:     filepath.Join(root, "project"),
		desiredRoot: filepath.Join(root, "project", "desired"),
		dump:        filepath.Join(root, "project", "dump"),
	}
	fixture.left = filepath.Join(fixture.workdir, "left.hcl")
	fixture.right = filepath.Join(fixture.workdir, "right.hcl")
	fixture.manifest = filepath.Join(fixture.desiredRoot, "manifest.hcl")

	writeFileT(t, filepath.Join(fixture.home, hclexpConfigFilename), `
global { ignore_column_order = true }
`)
	writeFileT(t, filepath.Join(fixture.workdir, hclexpConfigFilename), `
global { ignore_column_order = false }
diff   { ignore_column_order = true }
drift  { ignore_column_order = true }
plan   { ignore_column_order = true }
web    { ignore_column_order = true }
`)
	writeFileT(t, fixture.left, planColumnOrderHCL("a", "b"))
	writeFileT(t, fixture.right, planColumnOrderHCL("b", "a"))
	writeFileT(t, fixture.manifest, `role "ops" {
  env "prod" { layers = ["schema.hcl"] }
}
`)
	writeFileT(t, filepath.Join(fixture.desiredRoot, "schema.hcl"), planColumnOrderHCL("b", "a"))
	writeFileT(t, filepath.Join(fixture.dump, "node-a.hcl"), configDumpNode("node-a", "a", "a", "b"))
	writeFileT(t, filepath.Join(fixture.dump, "node-b.hcl"), configDumpNode("node-b", "b", "b", "a"))
	return fixture
}

func configDumpNode(node, replica, first, second string) string {
	return `node "` + node + `" {
  macros = { cluster = "ops", hostClusterRole = "ops", shard = "1", replica = "` + replica + `" }
}
` + planColumnOrderHCL(first, second)
}

func TestHCLExpConfigEveryComparisonCommandEndToEnd(t *testing.T) {
	fixture := newHCLExpConfigE2EFixture(t)

	t.Run("diff", func(t *testing.T) {
		stdout, stderr, err := runHCLExpConfigCLI(fixture, "diff", "-left", fixture.left, "-right", fixture.right)
		require.NoError(t, err, stderr)
		assert.Equal(t, "no differences\n", stdout)
		assert.Empty(t, stderr, "non-terminal output contains no config status")

		stdout, _, err = runHCLExpConfigCLI(fixture, "diff", "-left", fixture.left, "-right", fixture.right, "-ignore-column-order=false")
		require.NoError(t, err)
		assert.Contains(t, stdout, "column_order")
	})

	t.Run("plan dump", func(t *testing.T) {
		args := []string{
			"-manifest", fixture.manifest, "-layer-root", fixture.desiredRoot,
			"-env", "prod", "-dump", fixture.dump,
		}
		stdout, stderr, err := runHCLExpConfigCLI(fixture, "plan", args...)
		require.NoError(t, err, stderr)
		var clean hclload.PlanResult
		require.NoError(t, json.Unmarshal([]byte(stdout), &clean), stdout)
		assert.Empty(t, clean.Unsafe)
		require.Len(t, clean.Roles, 1)
		assert.Empty(t, clean.Roles[0].Objects)

		stdout, _, err = runHCLExpConfigCLI(fixture, "plan", append(args, "-ignore-column-order=false")...)
		require.NoError(t, err)
		var orderSensitive hclload.PlanResult
		require.NoError(t, json.Unmarshal([]byte(stdout), &orderSensitive), stdout)
		assert.NotEmpty(t, orderSensitive.Unsafe)
		assert.NotEmpty(t, orderSensitive.Roles[0].Objects)
	})

	t.Run("drift", func(t *testing.T) {
		stdout, stderr, err := runHCLExpConfigCLI(fixture, "drift", "-dir", fixture.dump)
		require.NoError(t, err, stderr)
		assert.Contains(t, stdout, "OK (all identical)")

		stdout, _, err = runHCLExpConfigCLI(fixture, "drift", "-dir", fixture.dump, "-ignore-column-order=false")
		require.Error(t, err, "order-sensitive drift exits non-zero")
		assert.Contains(t, stdout, "drifting")
	})

	t.Run("web dump", func(t *testing.T) {
		addr := reserveLoopbackAddress(t)
		cmd := hclexpConfigCommand(fixture, "web", "-dump", fixture.dump, "-addr", addr, "-reload-interval", "0")
		var stdout, stderr bytes.Buffer
		cmd.Stdout, cmd.Stderr = &stdout, &stderr
		require.NoError(t, cmd.Start())
		defer func() {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
		}()

		body := fetchEventually(t, "http://"+addr+"/object-diffs")
		assert.Contains(t, body, `name="ignore_column_order" value="1" checked`)
		assert.Contains(t, body, `<strong>0</strong><span>different</span>`)
	})
}

func TestHCLExpConfigCLITrueOverridesFalseEndToEnd(t *testing.T) {
	fixture := newHCLExpConfigE2EFixture(t)
	writeFileT(t, filepath.Join(fixture.workdir, hclexpConfigFilename), `global { ignore_column_order = false }`)
	stdout, stderr, err := runHCLExpConfigCLI(
		fixture, "diff", "-left", fixture.left, "-right", fixture.right, "-ignore-column-order=true",
	)
	require.NoError(t, err, stderr)
	assert.Equal(t, "no differences\n", stdout)
}

func TestHCLExpConfigHomeDiscoveryEndToEnd(t *testing.T) {
	fixture := newHCLExpConfigE2EFixture(t)
	writeFileT(t, filepath.Join(fixture.workdir, hclexpConfigFilename), "# no project overrides\n")
	stdout, stderr, err := runHCLExpConfigCLI(fixture, "diff", "-left", fixture.left, "-right", fixture.right)
	require.NoError(t, err, stderr)
	assert.Equal(t, "no differences\n", stdout, "the home global setting is applied")
}

func TestHCLExpConfigInvalidFileFailsClosedEndToEnd(t *testing.T) {
	fixture := newHCLExpConfigE2EFixture(t)
	writeFileT(t, filepath.Join(fixture.workdir, hclexpConfigFilename), `global { ignore_column_orders = true }`)
	stdout, stderr, err := runHCLExpConfigCLI(fixture, "diff", "-left", fixture.left, "-right", fixture.right)
	require.Error(t, err)
	assert.Empty(t, stdout)
	assert.Contains(t, stderr, filepath.Join(fixture.workdir, hclexpConfigFilename))
	assert.Contains(t, stderr, "Unsupported argument")
}

func runHCLExpConfigCLI(fixture hclexpConfigE2EFixture, command string, args ...string) (string, string, error) {
	cmd := hclexpConfigCommand(fixture, command, args...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	err := cmd.Run()
	return stdout.String(), stderr.String(), err
}

func hclexpConfigCommand(fixture hclexpConfigE2EFixture, command string, args ...string) *exec.Cmd {
	commandArgs := []string{"-test.run=^TestHCLExpConfigCLIProcess$", "--", command}
	commandArgs = append(commandArgs, args...)
	cmd := exec.Command(os.Args[0], commandArgs...)
	cmd.Dir = fixture.workdir
	cmd.Env = hclexpConfigTestEnv(fixture.home, "HCLEXP_CONFIG_HELPER=1")
	return cmd
}

func hclexpConfigTestEnv(home string, extra ...string) []string {
	overridden := map[string]bool{"HOME": true}
	for _, entry := range extra {
		if key, _, ok := strings.Cut(entry, "="); ok {
			overridden[key] = true
		}
	}
	env := make([]string, 0, len(os.Environ())+1+len(extra))
	for _, entry := range os.Environ() {
		key, _, _ := strings.Cut(entry, "=")
		if !overridden[key] {
			env = append(env, entry)
		}
	}
	env = append(env, "HOME="+home)
	return append(env, extra...)
}

func reserveLoopbackAddress(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := listener.Addr().String()
	require.NoError(t, listener.Close())
	return addr
}

func fetchEventually(t *testing.T, url string) string {
	t.Helper()
	client := &http.Client{Timeout: time.Second}
	deadline := time.Now().Add(10 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		response, err := client.Get(url)
		if err == nil {
			body, readErr := io.ReadAll(response.Body)
			_ = response.Body.Close()
			if readErr == nil && response.StatusCode == http.StatusOK {
				return string(body)
			}
			lastErr = readErr
		} else {
			lastErr = err
		}
		time.Sleep(20 * time.Millisecond)
	}
	require.NoError(t, lastErr)
	return ""
}
