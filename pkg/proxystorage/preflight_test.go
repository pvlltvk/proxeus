package proxystorage

import (
	"strings"
	"testing"

	"github.com/prometheus/common/model"

	"github.com/jacksontj/promxy/pkg/servergroup"
)

func TestValidateUniqueServerGroupLabels(t *testing.T) {
	tests := []struct {
		name        string
		groups      []*servergroup.Config
		wantErr     bool
		errContains []string // all of these substrings must appear in the error
	}{
		{
			name: "two groups with identical labels — expect collision error naming both groups",
			groups: []*servergroup.Config{
				{Name: "sg-0", Labels: model.LabelSet{"backend": "thanos"}},
				{Name: "sg-1", Labels: model.LabelSet{"backend": "thanos"}},
			},
			wantErr:     true,
			errContains: []string{"sg-0", "sg-1", "collision"},
		},
		{
			name: "three groups where two share labels — error names both colliders",
			groups: []*servergroup.Config{
				{Name: "sg-0", Labels: model.LabelSet{"backend": "thanos"}},
				{Name: "sg-1", Labels: model.LabelSet{"backend": "vm"}},
				{Name: "sg-3", Labels: model.LabelSet{"backend": "thanos"}},
			},
			wantErr:     true,
			errContains: []string{"sg-0", "sg-3", "collision"},
		},
		{
			name: "one group with empty labels — expect error",
			groups: []*servergroup.Config{
				{Name: "sg-0", Labels: nil},
			},
			wantErr:     true,
			errContains: []string{"sg-0", "empty labels"},
		},
		{
			name: "one group with empty label set (non-nil) — expect error",
			groups: []*servergroup.Config{
				{Name: "sg-0", Labels: model.LabelSet{}},
			},
			wantErr:     true,
			errContains: []string{"sg-0", "empty labels"},
		},
		{
			name: "two groups with distinct labels — no error",
			groups: []*servergroup.Config{
				{Name: "sg-0", Labels: model.LabelSet{"backend": "thanos"}},
				{Name: "sg-1", Labels: model.LabelSet{"backend": "vm"}},
			},
			wantErr: false,
		},
		{
			name: "three groups all distinct — no error",
			groups: []*servergroup.Config{
				{Name: "sg-0", Labels: model.LabelSet{"backend": "thanos"}},
				{Name: "sg-1", Labels: model.LabelSet{"backend": "vm"}},
				{Name: "sg-2", Labels: model.LabelSet{"backend": "cortex"}},
			},
			wantErr: false,
		},
		{
			name:    "empty group list — no error",
			groups:  []*servergroup.Config{},
			wantErr: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := validateUniqueServerGroupLabels(tc.groups)
			if tc.wantErr {
				if err == nil {
					t.Fatal("expected an error but got nil")
				}
				for _, sub := range tc.errContains {
					if !strings.Contains(err.Error(), sub) {
						t.Errorf("error %q does not contain expected substring %q", err.Error(), sub)
					}
				}
				return
			}
			if err != nil {
				t.Fatalf("expected no error but got: %v", err)
			}
		})
	}
}
