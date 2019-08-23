package vstate

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/leveldorado/vstate/user"
)

func TestServiceChangeRole(t *testing.T) {
	vehicleID := uuid.New().String()
	s := Service{}
	for _, el := range []struct {
		state         State
		role          user.Role
		shouldBeError bool
	}{
		{
			state:         BatteryLow,
			role:          user.RoleEndUser,
			shouldBeError: true,
		},
		{
			state:         BatteryLow,
			role:          user.RoleHunter,
			shouldBeError: true,
		},
		{
			state: BatteryLow,
			role:  user.RoleAdmin,
		},
		{
			state:         Bounty,
			role:          user.RoleEndUser,
			shouldBeError: true,
		},
		{
			state:         Bounty,
			role:          user.RoleHunter,
			shouldBeError: true,
		},
		{
			state: Bounty,
			role:  user.RoleAdmin,
		},
		{
			state: Ready,
			role:  user.RoleHunter,
		},
		{
			state: Ready,
			role:  user.RoleEndUser,
		},
		{
			state: Ready,
			role:  user.RoleAdmin,
		},
		{
			state: Riding,
			role:  user.RoleEndUser,
		},
		{
			state:         Riding,
			role:          user.RoleHunter,
			shouldBeError: true,
		},
		{
			state: Riding,
			role:  user.RoleAdmin,
		},
		{
			state:         Collected,
			role:          user.RoleEndUser,
			shouldBeError: true,
		},
		{
			state: Collected,
			role:  user.RoleHunter,
		},
		{
			state: Collected,
			role:  user.RoleAdmin,
		},
		{
			state:         Dropped,
			role:          user.RoleEndUser,
			shouldBeError: true,
		},
		{
			state: Dropped,
			role:  user.RoleHunter,
		},
		{
			state: Dropped,
			role:  user.RoleAdmin,
		},
		{
			state: ServiceMode,
			role:  user.RoleAdmin,
		},
		{
			state: Terminated,
			role:  user.RoleAdmin,
		},
		{
			state: Unknown,
			role:  user.RoleAdmin,
		},
	} {
		err := s.ChangeState(context.Background(), vehicleID, el.state, el.role)
		if el.shouldBeError && err == nil {
			t.Errorf("changing to state %s by % should be error", el.state, el.role)
		}
		if !el.shouldBeError && err != nil {
			t.Errorf("changing to state %s by % should not be error", el.state, el.role)
		}
	}
}
