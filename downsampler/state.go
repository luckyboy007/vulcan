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
	"time"

	"github.com/digitalocean/vulcan/model"
)

func (d *Downsampler) updateLastWrite(fqmn string, t int64) {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	d.lastWrite[fqmn] = t

	d.stateHashLength.Set(float64(len(d.lastWrite)))
}

func (d *Downsampler) updateLastWrites(tsb model.TimeSeriesBatch) {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	for _, ts := range tsb {
		d.lastWrite[ts.ID()] = ts.Samples[0].TimestampMS
	}

	d.stateHashLength.Set(float64(len(d.lastWrite)))
}

func (d *Downsampler) getLastWrite(fqmn string) (timestampMS int64, ok bool) {
	d.mutex.RLock()
	defer d.mutex.RUnlock()

	t, ok := d.lastWrite[fqmn]
	return t, ok
}

func (d *Downsampler) cleanLastWrite(now int64, diff int64) {
	var toDelete = map[string]int64{}

	d.mutex.RLock()
	for fqmn, ts := range d.lastWrite {
		if now-ts > diff {
			toDelete[fqmn] = ts
		}
	}
	d.mutex.RUnlock()

	if len(toDelete) > 0 {
		d.mutex.Lock()
		for fqmn, ts := range toDelete {
			// Only delete if the timestamp of the fqmn is still the same one that
			// we measured against when we marked the item for deletion.
			if ts == d.lastWrite[fqmn] {
				delete(d.lastWrite, fqmn)
			}
		}
		d.mutex.Unlock()

		d.stateHashDeletes.Add(float64(len(toDelete)))
	}
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
