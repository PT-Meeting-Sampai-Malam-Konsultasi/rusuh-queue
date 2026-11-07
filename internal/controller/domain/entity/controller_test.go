package entity

import (
	"strings"
	"testing"
)

func TestDefaultConfigHasAUniqueTaskID(t *testing.T) {
	a, b := DefaultConfig(), DefaultConfig()
	if a.TaskID == b.TaskID || !strings.Contains(a.TaskID, "-") || a.TickEvery <= 0 || a.LeaderTTL <= a.TickEvery {
		t.Fatalf("configs = %+v, %+v", a, b)
	}
}
