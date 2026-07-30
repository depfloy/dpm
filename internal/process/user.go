package process

import (
	"fmt"
	"os"
	"os/user"
	"strconv"
	"strings"
	"syscall"
)

// Running a managed process as somebody other than root.
//
// The daemon runs as root, and until now every process it started inherited
// that: a customer's Node application, reachable from the internet, ran with
// uid 0. An RCE or a path traversal in their code was root on a box that hosts
// other customers' projects. ProcessConfig.User existed and was documented, but
// nothing read it — the field was accepted and discarded.
//
// Two things have to happen together, and doing only the first is worse than
// doing neither: the process gets the target user's credentials, and its
// environment stops claiming to be root's. HOME is the one that bites — it is
// inherited from the daemon as /root, and a Node build writing its cache to
// $HOME fails with EACCES against a directory it can no longer read. That
// failure surfaces as the application not starting, which reads like the
// release being broken.

// credentialFor resolves a user name to the credentials a child process should
// run under.
//
// Returns nil when no user is configured — that is the old behaviour, and
// existing processes must keep working across an upgrade. An unknown user is an
// error rather than a silent fallback: a security control that quietly turns
// itself off when misconfigured is not a control, and root is exactly what we
// are trying to stop.
func credentialFor(username string) (*syscall.Credential, error) {
	if strings.TrimSpace(username) == "" {
		return nil, nil
	}

	// Dropping privileges requires having them. If the daemon is not root the
	// process already runs as whoever started it, so there is nothing to drop
	// and nothing to fail over.
	if os.Geteuid() != 0 {
		return nil, nil
	}

	target, err := user.Lookup(username)
	if err != nil {
		return nil, fmt.Errorf("look up user %q: %w", username, err)
	}

	return credentialFrom(target.Uid, target.Gid)
}

// credentialFrom builds the credential from an already-resolved uid and gid.
//
// Split out from credentialFor so the part that decides what the child gets can
// be tested without being root — the lookup and the euid check cannot, and
// leaving everything behind them means the group handling is only ever
// exercised on a production box.
func credentialFrom(uidStr, gidStr string) (*syscall.Credential, error) {
	uid, err := strconv.ParseUint(uidStr, 10, 32)
	if err != nil {
		return nil, fmt.Errorf("parse uid %q: %w", uidStr, err)
	}

	gid, err := strconv.ParseUint(gidStr, 10, 32)
	if err != nil {
		return nil, fmt.Errorf("parse gid %q: %w", gidStr, err)
	}

	// Groups is set explicitly rather than left empty so the child does not keep
	// root's supplementary groups. With NoSetGroups false and this list, setgroups
	// replaces them with the user's own primary group.
	return &syscall.Credential{
		Uid:    uint32(uid),
		Gid:    uint32(gid),
		Groups: []uint32{uint32(gid)},
	}, nil
}

// environForUser rewrites the identity variables in env so they describe the
// user the process will actually run as.
//
// The daemon's environment is inherited wholesale, so without this HOME stays
// /root while the process runs as depfloy. Anything that writes to $HOME —
// npm's cache, Next's telemetry file, a framework's temp directory — then tries
// to write into root's home and fails on permissions.
//
// Only the identity variables are touched. Everything else the process was
// given, including PATH and whatever Depfloy injected, is left alone.
func environForUser(env []string, username string) []string {
	if strings.TrimSpace(username) == "" {
		return env
	}

	target, err := user.Lookup(username)
	if err != nil {
		// The caller resolves credentials from the same name and fails there, so
		// this cannot silently start a process with the wrong identity. Returning
		// the environment unchanged keeps this function total.
		return env
	}

	replacements := map[string]string{
		"HOME":    target.HomeDir,
		"USER":    target.Username,
		"LOGNAME": target.Username,
	}

	out := make([]string, 0, len(env)+len(replacements))

	for _, entry := range env {
		name, _, found := strings.Cut(entry, "=")
		if found {
			if _, replaced := replacements[name]; replaced {
				continue
			}
		}
		out = append(out, entry)
	}

	for name, value := range replacements {
		out = append(out, name+"="+value)
	}

	return out
}
