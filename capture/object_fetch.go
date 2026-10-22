package fetch

import (
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	"regexp"
	"strings"
)

// Rclone reads the existing named remote credential; stderr never leaves the child.
func (r *run) fetchObject(ctx context.Context, remote, bucket, key string) ([]byte, error) {
	if !regexp.MustCompile(`^[A-Za-z][A-Za-z0-9_-]*$`).MatchString(remote) || bucket == "" || strings.Contains(bucket, "/") || key == "" {
		return nil, errors.New("B2 object location invalid")
	}
	binary := r.env["QMD_RCLONE_BIN"]
	if binary == "" {
		binary = "rclone"
	}
	args := []string{"cat", remote + ":" + bucket + "/" + key, "--retries", "1", "--low-level-retries", "2", "--contimeout", "30s", "--timeout", "120s"}
	if config := r.env["QMD_RCLONE_CONFIG"]; config != "" {
		args = append(args, "--config", config)
	}
	cmd := exec.CommandContext(ctx, binary, args...)
	cmd.Env = os.Environ()
	cmd.Stderr = io.Discard
	var output boundedBuffer
	cmd.Stdout = &output
	if cmd.Run() != nil {
		return nil, errors.New("B2 object fetch failed")
	}
	return output.Bytes(), nil
}
