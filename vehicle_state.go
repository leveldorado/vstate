package vstate

import (
	"context"

	"github.com/leveldorado/vstate/user"
)

/*
State of vehicle
*/
type State string

/*
Supported states of vehicle
*/
const (
	/*
	   Operational statutes
	*/
	// The vehicle is operational and can be claimed by an end­user
	Ready State = "Ready"
	// The vehicle is low on battery but otherwise operational.
	// The vehicle cannot be claimed by an end­user but can be claimed by a hunter.
	BatteryLow State = "Battery_low"
	// Only available for “Hunters” to be picked up for charging.
	Bounty State = "Bounty"
	// An end user is currently using this vehicle; it can not be claimed by another user or hunter.
	Riding State = "Riding"
	// A hunter has picked up a vehicle for charging.
	Collected State = "Collected"
	// A hunter has returned a vehicle after being charged.
	Dropped State = "Dropped"
	/*
	   Not commissioned for service , not claimable by either end­users nor hunters.
	*/
	ServiceMode State = "Service_mode"
	Terminated  State = "Terminated"
	Unknown     State = "Unknown"
)

/*
Service manages vehicle state
*/
type Service struct{}

/*
ChangeState changes vehicle state
 if state is not valid returns an error
 if state is valid returns nil
*/
func (s *Service) ChangeState(ctx context.Context, vehicleID string, st State, role user.Role) error {
	return nil
}

/*
GetState returns current state of vehicle
*/
func (s *Service) GetState(ctx context.Context, vehicleID string) (State, error) {
	return "", nil
}
