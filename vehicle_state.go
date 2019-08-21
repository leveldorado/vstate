package vstate



type State string


const (
	/*
	   Operational statutes
	 */
	// The vehicle is operational and can be claimed by an end­user
	StateReady = "Ready"
	// The vehicle is low on battery but otherwise operational.
	// The vehicle cannot be claimed by an end­user but can be claimed by a hunter.
	StateBatteryLow = "Battery_low"
	// Only available for “Hunters” to be picked up for charging.
	StateBounty = "Bounty"
	// An end user is currently using this vehicle; it can not be claimed by another user or hunter.
	StateRiding = "Riding"
	// A hunter has picked up a vehicle for charging.
	StateCollected = "Collected"
	// A hunter has returned a vehicle after being charged.
	StateDropped = "Dropped"
	/*
	   Not commissioned for service , not claimable by either end­users nor hunters.
	 */
	StateServiceMode = "Service_mode"
	StateTerminated = "Terminated"
	StateUnknown = "Unknown"
)
