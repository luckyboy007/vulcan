// Copyright 2016 The Vulcan Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package downsampler

import (
	"sync/atomic"
	"time"

	"github.com/digitalocean/vulcan/model"
)

func (d *Downsampler) appendLastWrite(fqmn string, t int64) {
	d.mutex.Lock()
	d.lastWrite[fqmn] = int64ToPt(t)
	d.mutex.Unlock()
}

func (d *Downsampler) updateLastWrite(fqmn string, t int64) {
	a, ok := d.getLastWrite(fqmn)
	if !ok {
		d.appendLastWrite(fqmn, t)
		return
	}

	atomic.SwapInt64(a, t)
	d.stateHashLength.Set(float64(len(d.lastWrite)))
}

func (d *Downsampler) updateLastWrites(tsb model.TimeSeriesBatch) {
	for _, ts := range tsb {
		d.updateLastWrite(ts.ID(), ts.Samples[0].TimestampMS)
	}
}

// getLastWrite retrives an address for atomic operations. Because the read lock
// is released before the address is returned to the caller, there is a potential
// race condition where the caller is doing updating the returned int64 pointer
// just before a cleanUp sweep occurs and removes occurs, in which case there
// will be a extra read to the data storage to retrieve last timestamp.  This
// scenario should be unlikely.
func (d *Downsampler) getLastWrite(fqmn string) (tsAddr *int64, ok bool) {
	d.mutex.RLock()
	a, ok := d.lastWrite[fqmn]
	d.mutex.RUnlock()

	return a, ok
}

func (d *Downsampler) getLastWriteValue(fqmn string) (timestampMS int64, ok bool) {
	a, ok := d.getLastWrite(fqmn)
	if !ok {
		return 0, ok
	}

	return atomic.LoadInt64(a), ok
}

func (d *Downsampler) cleanLastWrite(now int64, diff int64) {
	var toDelete []string

	// Use a write lock here to reduce potential of race condition where an atomic
	// update occurs just before the clean up sweep begins.  However, write locking
	// here also introduces a longer duration of read locks from goroutines trying
	// to read lastWrite.
	d.mutex.RLock()
	for fqmn, ts := range d.lastWrite {
		if now-*ts > diff {
			toDelete = append(toDelete, fqmn)
		}
	}
	d.mutex.RUnlock()

	d.mutex.Lock()
	if len(toDelete) > 0 {
		for _, fqmn := range toDelete {
			delete(d.lastWrite, fqmn)
		}
	}
	d.mutex.Unlock()

	d.stateHashDeletes.Add(float64(len(toDelete)))
}

func (d *Downsampler) getLastFrDisk(fqmn string) (updatedAtMS int64, err error) {
	d.readCount.WithLabelValues("disk").Inc()

	s, err := d.reader.GetLastSample(fqmn)
	if err != nil {
		return 0, err
	}

	return s.TimestampMS, nil
}

// cleanUp sweeps through the downsampler lastWrite state, and removes
// records older than 1.5X the resolution duration.
func (d *Downsampler) cleanUp() {
	var (
		diffd = time.Duration(d.resolution*3/2) * time.Millisecond
		t     = time.NewTicker(diffd)
		diffi = diffd.Nanoseconds() / int64(time.Millisecond)
	)

	for {
		select {
		case <-t.C:
			d.cleanLastWrite(timeToMS(time.Now()), diffi)

		case <-d.done:
			t.Stop()
			return
		}
	}
}

func timeToMS(t time.Time) int64 {
	return t.UnixNano() / int64(time.Millisecond)
}

func int64ToPt(i int64) *int64 {
	return &i
}
