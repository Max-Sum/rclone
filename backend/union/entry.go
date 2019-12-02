package union

import (
	"context"
	"bufio"
	"io"
	"sync"
	"time"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/backend/union/upstream"
)

// Object describes a union Object
//
// This is a wrapped object which returns the Union Fs as its parent
type Object struct {
	*upstream.Object
	fs *Fs // what this object is part of
	co []upstream.Entry
}

// Directory describes a union Directory
//
// This is a wrapped object contains all candidates
type Directory struct {
	*upstream.Directory
	cd []upstream.Entry
}

type entry interface {
	upstream.Entry
	candidates() []upstream.Entry
}

// UnWrap returns the Object that this Object is wrapping or
// nil if it isn't wrapping anything
func (o *Object) UnWrap() *upstream.Object {
	return o.Object
}

// Fs returns the union Fs as the parent
func (o *Object) Fs() fs.Info {
	return o.fs
}

func (o *Object) candidates() []upstream.Entry {
	return o.co
}

func (d *Directory) candidates() []upstream.Entry {
	return d.cd
}

// Update in to the object with the modTime given of the given size
//
// When called from outside a Fs by rclone, src.Size() will always be >= 0.
// But for unknown-sized objects (indicated by src.Size() == -1), Upload should either
// return an error or update the object properly (rather than e.g. calling panic).
func (o *Object) Update(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) error {
	f := o.Fs().(*Fs)
	entries, err := f.actionEntries(o.candidates()...)
	if err != nil {
		return err
	}
	if len(entries) == 1 {
		obj := entries[0].(*upstream.Object)
		return obj.Update(ctx, in, src, options...)
	}
	// Get multiple reader
	readers := make([]io.Reader, len(entries))
	writers := make([]io.Writer, len(entries))
	errs := make([]error, len(entries) + 1)
	for i := range entries {
		r, w := io.Pipe()
		bw := bufio.NewWriter(w)
		readers[i], writers[i] = r, bw
		defer w.Close()
	}
	go func() {
		mw := io.MultiWriter(writers...)
        _, errs[len(entries)] = io.Copy(mw, in)
		for _, bw := range writers {
			bw.(*bufio.Writer).Flush()
		}
	}()
	// Multi-threading
	multithread(len(entries), func(i int){
		if o, ok := entries[i].(*upstream.Object); ok {
			errs[i] = o.Update(ctx, readers[i], src, options...)
		} else {
			errs[i] = fs.ErrorNotAFile
		}
	})
	// Get an object for future operation
	for _, err := range errs {
		if err != nil {
			return err
		}
	}
	return nil
}

// Remove candidate objects selected by ACTION policy
func (o *Object) Remove(ctx context.Context) error {
	f := o.Fs().(*Fs)
	entries, err := f.actionEntries(o.candidates()...)
	if err != nil {
		return err
	}
	errs := make([]error, len(entries))
	multithread(len(entries), func(i int){
		if o, ok := entries[i].(*upstream.Object); ok {
			errs[i] = o.Remove(ctx)
		} else {
			errs[i] = fs.ErrorNotAFile
		}
	})
	for _, err := range errs {
		if err != nil {
			return err
		}
	}
	return nil
}
// SetModTime sets the metadata on the object to set the modification date
func (o *Object) SetModTime(ctx context.Context, t time.Time) error {
	f := o.Fs().(*Fs)
	entries, err := f.actionEntries(o.candidates()...)
	if err != nil {
		return err
	}
	var wg sync.WaitGroup
	errs := make([]error, len(entries))
	multithread(len(entries), func(i int){
		if o, ok := entries[i].(*upstream.Object); ok {
			errs[i] = o.SetModTime(ctx, t)
		} else {
			errs[i] = fs.ErrorNotAFile
		}
	})
	wg.Wait()
	for _, err := range errs {
		if err != nil {
			return err
		}
	}
	return nil
}

// ModTime returns the modification date of the directory
// It returns the latest ModTime of all candidates
func (d *Directory) ModTime(ctx context.Context) (t time.Time) {
	entries := d.candidates()
	times := make([]time.Time, len(entries))
	multithread(len(entries), func(i int){
		times[i] = entries[i].ModTime(ctx)
	})
	for _, ti := range times {
		if t.Before(ti) {
			t = ti
		}
	}
	return t
}

// Size returns the size of the directory
// It returns the sum of all candidates
func (d *Directory) Size() (s int64) {
	for _, e := range d.candidates() {
		s += e.Size()
	}
	return s
}