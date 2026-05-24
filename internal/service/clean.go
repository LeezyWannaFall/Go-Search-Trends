package service

import (
	"time"
)

func (s *TrendingService) cleanupOldBuckets() {
	tNow := time.Now().Unix() / 60
	border := tNow - 4

	s.mu.Lock()
	if tNow == s.lastCleanup {
		s.mu.Unlock()
		return
	}
	s.lastCleanup = tNow
	for key := range s.buckets {
		if key < border {
			delete(s.buckets, key)
		}
	}
	s.mu.Unlock()
}