package process

import (
	"os"
	"os/user"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/depfloy/dpm/pkg/config"
)

// A customer's Node application ran as root because ProcessConfig.User was
// accepted and never read. These cover the two halves of dropping it: the
// credentials the child gets, and the environment that has to move with them.
//
// The setuid itself cannot be exercised without being root, so the tests below
// pin the decisions that lead to it. The one test that needs root says so and
// skips.

func TestCredentialForEmptyUserKeepsOldBehaviour(t *testing.T) {
	// Processes created before this existed carry no user. They have to keep
	// starting across an upgrade, so no user means no credential rather than a
	// default that changes who they run as.
	credential, err := credentialFor("")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if credential != nil {
		t.Fatalf("expected no credential for an empty user, got %+v", credential)
	}

	if credential, err = credentialFor("   "); err != nil || credential != nil {
		t.Fatalf("whitespace should read as no user: credential=%+v err=%v", credential, err)
	}
}

func TestCredentialForUnknownUserFails(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("credentialFor returns early when not root; the lookup only runs as root")
	}

	// Falling back to root here would turn a typo into a silent privilege
	// escalation, and the process would report itself as started.
	if _, err := credentialFor("nosuchuser-dpm-test"); err == nil {
		t.Fatal("expected an unknown user to be an error, not a fallback to root")
	}
}

func TestCredentialForResolvesCurrentUser(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("dropping privileges requires having them; run as root to exercise this")
	}

	current, err := user.Current()
	if err != nil {
		t.Fatalf("user.Current: %v", err)
	}

	credential, err := credentialFor(current.Username)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if credential == nil {
		t.Fatal("expected a credential for a real user")
	}

	uid, _ := strconv.ParseUint(current.Uid, 10, 32)
	if credential.Uid != uint32(uid) {
		t.Fatalf("uid: got %d, want %d", credential.Uid, uid)
	}

	// Empty groups would leave root's supplementary groups in place, which is
	// most of what dropping the user was for.
	if len(credential.Groups) == 0 {
		t.Fatal("expected supplementary groups to be replaced, not inherited")
	}
}

func TestCredentialForNonRootDoesNotFail(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("this covers the non-root daemon case")
	}

	// A daemon that is not root has no privileges to drop. Returning an error
	// would stop processes from starting in development, where dpm runs as an
	// ordinary user.
	credential, err := credentialFor("root")
	if err != nil {
		t.Fatalf("unexpected error when not root: %v", err)
	}
	if credential != nil {
		t.Fatalf("expected no credential when the daemon is not root, got %+v", credential)
	}
}

func TestEnvironForUserRewritesHome(t *testing.T) {
	current, err := user.Current()
	if err != nil {
		t.Fatalf("user.Current: %v", err)
	}

	// The shape the daemon actually passes down: root's identity, inherited
	// wholesale. HOME is the one that breaks things — a Node build writing its
	// cache to /root fails on permissions once the process is no longer root,
	// and the application simply does not come up.
	env := []string{
		"HOME=/root",
		"USER=root",
		"LOGNAME=root",
		"PATH=/usr/local/bin:/usr/bin",
		"NODE_ENV=production",
	}

	got := environForUser(env, current.Username)
	values := envMap(got)

	if values["HOME"] != current.HomeDir {
		t.Fatalf("HOME: got %q, want %q", values["HOME"], current.HomeDir)
	}
	if values["USER"] != current.Username {
		t.Fatalf("USER: got %q, want %q", values["USER"], current.Username)
	}
	if values["LOGNAME"] != current.Username {
		t.Fatalf("LOGNAME: got %q, want %q", values["LOGNAME"], current.Username)
	}

	// Everything else the process was given has to survive, including whatever
	// Depfloy injected.
	if values["PATH"] != "/usr/local/bin:/usr/bin" {
		t.Fatalf("PATH was modified: %q", values["PATH"])
	}
	if values["NODE_ENV"] != "production" {
		t.Fatalf("NODE_ENV was modified: %q", values["NODE_ENV"])
	}
}

func TestEnvironForUserLeavesNoStaleDuplicate(t *testing.T) {
	current, err := user.Current()
	if err != nil {
		t.Fatalf("user.Current: %v", err)
	}

	// Appending without removing would leave HOME twice. Which one wins is up to
	// the program reading it, so the old value has to be gone rather than
	// shadowed.
	got := environForUser([]string{"HOME=/root", "HOME=/root"}, current.Username)

	count := 0
	for _, entry := range got {
		if strings.HasPrefix(entry, "HOME=") {
			count++
		}
	}

	if count != 1 {
		t.Fatalf("expected exactly one HOME entry, got %d: %v", count, got)
	}
}

func TestEnvironForUserEmptyUserChangesNothing(t *testing.T) {
	env := []string{"HOME=/root", "USER=root"}

	got := environForUser(env, "")

	if len(got) != len(env) || got[0] != env[0] || got[1] != env[1] {
		t.Fatalf("environment was modified for an empty user: %v", got)
	}
}

func envMap(env []string) map[string]string {
	out := make(map[string]string, len(env))
	for _, entry := range env {
		name, value, found := strings.Cut(entry, "=")
		if found {
			out[name] = value
		}
	}

	return out
}

func TestCredentialFromReplacesSupplementaryGroups(t *testing.T) {
	// The half of credentialFor that does not need root. Without this the group
	// handling would only ever run on a production box, which is where a mistake
	// costs the most.
	credential, err := credentialFrom("1001", "1001")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if credential.Uid != 1001 || credential.Gid != 1001 {
		t.Fatalf("got uid=%d gid=%d, want 1001/1001", credential.Uid, credential.Gid)
	}

	// An empty Groups leaves root's supplementary groups on the child, which
	// keeps most of the access we were trying to take away.
	if len(credential.Groups) != 1 || credential.Groups[0] != 1001 {
		t.Fatalf("expected groups to be replaced with the user's own, got %v", credential.Groups)
	}
}

func TestCredentialFromRejectsUnparseableIds(t *testing.T) {
	// Directory-backed users (LDAP, SSSD) can report non-numeric ids. Guessing
	// here would mean guessing which user the process runs as.
	if _, err := credentialFrom("not-a-number", "1001"); err == nil {
		t.Fatal("expected an error for a non-numeric uid")
	}

	if _, err := credentialFrom("1001", "not-a-number"); err == nil {
		t.Fatal("expected an error for a non-numeric gid")
	}
}

// TestStartRunsProcessAsConfiguredUser is the only test that proves the fix
// works. Everything above pins a decision; this one starts a process and asks
// the kernel who owns it.
//
// It needs root, because dropping privileges requires having them, and it needs
// Linux, because that is what the fleet runs. On a developer's machine it skips
// — run it on the canary box (or any Linux root shell) before shipping a
// binary:
//
//	sudo go test ./internal/process/ -run TestStartRunsProcessAsConfiguredUser -v
func TestStartRunsProcessAsConfiguredUser(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("the fleet runs Linux; setuid semantics here are not the ones that matter")
	}
	if os.Geteuid() != 0 {
		t.Skip("run as root to prove the process actually changes user")
	}

	target, err := user.Lookup("nobody")
	if err != nil {
		t.Skipf("no nobody user to drop to: %v", err)
	}

	manager, _ := testManager(t)
	outputDir := t.TempDir()
	// World-writable: nobody has to be able to write the result, which is itself
	// part of what is being checked — a process that cannot write anything looks
	// the same as one that never ran.
	if err := os.Chmod(outputDir, 0o777); err != nil {
		t.Fatalf("chmod: %v", err)
	}
	resultPath := outputDir + "/whoami"

	cfg := &config.ProcessConfig{
		Type:    "worker",
		Name:    "user-drop-probe",
		User:    "nobody",
		CWD:     outputDir,
		Command: "id -u > " + resultPath + "; sleep 5",
	}

	if err := manager.Start(cfg, nil); err != nil {
		t.Fatalf("start: %v", err)
	}
	t.Cleanup(func() { manager.Stop(cfg.Name) })

	var recorded []byte
	for i := 0; i < 50; i++ {
		if recorded, err = os.ReadFile(resultPath); err == nil && len(recorded) > 0 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	got := strings.TrimSpace(string(recorded))
	if got == "" {
		t.Fatal("the process wrote nothing; it either never started or could not write as the target user")
	}

	if got == "0" {
		t.Fatal("the process ran as root — the credential was not applied")
	}

	if got != target.Uid {
		t.Fatalf("process ran as uid %s, want %s (nobody)", got, target.Uid)
	}
}
