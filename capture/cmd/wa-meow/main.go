package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/05802/qmd2/capture/journal"
	"github.com/05802/qmd2/capture/receiver"

	_ "github.com/mattn/go-sqlite3"
	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/store/sqlstore"
)

func storePath(home string) (string, error) {
	root := filepath.Join(home, ".local/share/qmd/wa-meow")
	if e := os.MkdirAll(root, 0700); e != nil {
		return "", e
	}
	real, e := filepath.EvalSymlinks(root)
	if e != nil {
		return "", e
	}
	candidate := filepath.Join(real, "device.db")
	legacy := filepath.Join(home, ".local/share/wa/device.db")
	if r, e := filepath.EvalSymlinks(candidate); e == nil {
		candidate = r
	}
	if r, e := filepath.EvalSymlinks(legacy); e == nil {
		legacy = r
	}
	if candidate == legacy {
		return "", errors.New("refusing legacy WhatsApp device database")
	}
	return candidate, nil
}
func run() error {
	if len(os.Args) != 2 || os.Args[1] != "daemon" {
		return errors.New("usage: wa-meow daemon")
	}
	home, e := os.UserHomeDir()
	if e != nil {
		return e
	}
	db, e := storePath(home)
	if e != nil {
		return e
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	container, e := sqlstore.New(ctx, "sqlite3", "file:"+db+"?_foreign_keys=on", nil)
	if e != nil {
		return e
	}
	defer container.Close()
	device, e := container.GetFirstDevice(ctx)
	if e != nil {
		return e
	}
	if device.ID == nil {
		return errors.New("paired device required")
	}
	cli := whatsmeow.NewClient(device, nil)
	// whatsmeow stays in inactive receipt mode; this process never calls SendPresence.
	root := filepath.Dir(db)
	w, e := journal.Open(root, journal.FormatEpoch(time.Now()), device.ID.String(), device.ID.String(), receiver.Version, 64<<20)
	if e != nil {
		return e
	}
	defer w.Close()
	h := receiver.Handler{Journal: w}
	failures := make(chan error, 1)
	cli.AddEventHandler(func(evt any) {
		if err := h.Handle(evt); err != nil {
			select {
			case failures <- err:
			default:
			}
			stop()
		}
	})
	if e = cli.Connect(); e != nil {
		return e
	}
	defer cli.Disconnect()
	<-ctx.Done()
	select {
	case err := <-failures:
		return err
	default:
		return nil
	}
}

func main() {
	if e := run(); e != nil {
		fmt.Fprintln(os.Stderr, "wa-meow receiver failed")
		os.Exit(1)
	}
}
