package mapreduce

import "sync"

// A DoneChan is used as a channel that can be closed multiple times and wait for done.
type DoneChan struct {
	done chan struct{}
	once sync.Once
}

// NewDoneChan returns a DoneChan.
func NewDoneChan() *DoneChan {
	return &DoneChan{
		done: make(chan struct{}),
	}
}

// Close closes dc, it's safe to close more than once.
func (dc *DoneChan) Close() {
	dc.once.Do(func() {
		close(dc.done)
	})
}

// Done returns a channel that can be notified on dc closed.
func (dc *DoneChan) Done() chan struct{} {
	return dc.done
}
