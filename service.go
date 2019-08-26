package vstate

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/leveldorado/vstate/user"
	"github.com/pkg/errors"
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
func (s *Service) ChangeState(ctx context.Context, vehicleID string, st VehicleState, role user.Role) error {

	return nil
}

/*
GetState returns current state of vehicle
*/
func (s *Service) GetState(ctx context.Context, vehicleID string) (VehicleState, error) {
	return "", nil
}

/*
VehicleState of vehicle
*/
type VehicleState string

/*
Supported states of vehicle
*/
const (
	/*
	   Operational statutes
	*/
	// The vehicle is operational and can be claimed by an end­user
	Ready VehicleState = "Ready"
	// The vehicle is low on battery but otherwise operational.
	// The vehicle cannot be claimed by an end­user but can be claimed by a hunter.
	BatteryLow VehicleState = "Battery_low"
	// Only available for “Hunters” to be picked up for charging.
	Bounty VehicleState = "Bounty"
	// An end user is currently using this vehicle; it can not be claimed by another user or hunter.
	Riding VehicleState = "Riding"
	// A hunter has picked up a vehicle for charging.
	Collected VehicleState = "Collected"
	// A hunter has returned a vehicle after being charged.
	Dropped VehicleState = "Dropped"
	/*
	   Not commissioned for service , not claimable by either end­users nor hunters.
	*/
	ServiceMode VehicleState = "Service_mode"
	Terminated  VehicleState = "Terminated"
	Unknown     VehicleState = "Unknown"
)

type Vehicle struct {
	ID            string
	State         VehicleState
	LastUpdatedAt time.Time
	Handled       bool
	TimeLocation  time.Location
}

type vehicleHandlingLock struct {
	VehicleID string
	CreatedAt time.Time
}

type ErrPrimaryKeyDuplicated struct{}

func (ErrPrimaryKeyDuplicated) Error() string {
	return "Primary key duplicated"
}

type vehicleRepo interface {
	ListNotHandled(ctx context.Context, limit int) ([]Vehicle, error)
	UpdateState(ctx context.Context, id string, s VehicleState, t time.Time) error
	UpdateHandled(ctx context.Context, id string, handled bool, t time.Time) error
}

type vehicleHandlingLockRepo interface {
	Create(ctx context.Context, doc vehicleHandlingLock) error
	Delete(ctx context.Context, id string) error
}

type ChangeStateHandler struct {
	handledVehiclesLimit    int
	vehicleRepo             vehicleRepo
	vehicleHandlingLockRepo vehicleHandlingLockRepo
}

type StateHandlerQueue interface {
	RegisterVehicle(ctx context.Context, vehicleID string) error
	GetState(ctx context.Context, vehicleID string) (VehicleState, error)
	GetStateAndLock(ctx context.Context, vehicleID string) (s VehicleState, lockID string, err error)
	ChangeState(ctx context.Context, vehicleID, lockID string, s VehicleState) error
}

type DefaultStateHandlerQueue struct {
}

type vehicleStateQueue struct {
	currentState VehicleState
	in           chan<- stateChangeMessage
	out          <-chan stateChangeResult
}

type stateChangeMessage struct {
	LockID    string
	VehicleID string
	State     VehicleState
}

type stateChangeResult struct {
}

func (q *DefaultStateHandlerQueue) RegisterVehicle(ctx context.Context, vehicleID string) error {

}

func (h *ChangeStateHandler) Handle(ctx context.Context) (*StateHandlerQueue, error) {
	rp := &rollbacksPull{}
	defer rp.rollback()
	vehiclesToHandle, err := h.getVehiclesToHandle(ctx, rp)
	if err != nil {
		return nil, errors.Wrap(err, `failed to get vehicles to handle`)
	}
	q := DefaultStateHandlerQueue{}
	for _, v := range vehiclesToHandle {

	}
	rp.commit()
	return nil
}

func (h *ChangeStateHandler) handleStateChange(ctx context.Context, vehicleID string) {
	return
}

func (h *ChangeStateHandler) getVehiclesToHandle(ctx context.Context, rp *rollbacksPull) ([]Vehicle, error) {
	var vehiclesToHandle []Vehicle
	for {
		if len(vehiclesToHandle) == h.handledVehiclesLimit {
			break
		}
		limit := h.handledVehiclesLimit - len(vehiclesToHandle)
		vehicles, err := h.vehicleRepo.ListNotHandled(ctx, limit)
		if err != nil {
			return nil, errors.Wrapf(err, `failed to get list not handled vehicles: [limit: %d]`, limit)
		}
		if len(vehicles) == 0 {
			break
		}
		for _, v := range vehicles {
			success, err := h.acquireVehicleForHandling(ctx, v.ID, rp)
			if err != nil {
				return nil, errors.Wrapf(err, `failed to acquire vehicle for handling: [id: %s]`, v.ID)
			}
			if !success {
				continue
			}
			vehiclesToHandle = append(vehiclesToHandle, v)
		}
	}
	return vehiclesToHandle, nil
}

func (h *ChangeStateHandler) acquireVehicleForHandling(ctx context.Context, id string, rp *rollbacksPull) (bool, error) {
	lockEntry := vehicleHandlingLock{VehicleID: id, CreatedAt: time.Now().UTC()}
	err := h.vehicleHandlingLockRepo.Create(ctx, lockEntry)
	switch errors.Cause(err).(type) {
	case ErrPrimaryKeyDuplicated:
		return false, nil
	case nil:
	default:
		return false, errors.Wrapf(err, `failed to save vehicle handling lock: [doc: %+v]`, lockEntry)
	}
	rp.addRollback(func() error {
		return errors.Wrapf(h.vehicleHandlingLockRepo.Delete(ctx, id), `failed to delete vehicle handling lock: [id: %s]`, id)
	})
	if err = h.vehicleRepo.UpdateHandled(ctx, id, true, time.Now().UTC()); err != nil {
		return false, errors.Wrapf(err, `failed to update handled: [id: %s, handled: %v]`, id, true)
	}
	rp.addRollback(func() error {
		return errors.Wrapf(h.vehicleRepo.UpdateHandled(ctx, id, false, time.Now().UTC()), `failed to update vehicle: [id: %s, handled: %v]`, id, false)
	})
	return true, nil
}

type rollbacksPull struct {
	success   bool
	rollbacks []func() error
}

func (p *rollbacksPull) commit() {
	p.success = true
}

func (p *rollbacksPull) addRollback(f func() error) {
	p.rollbacks = append(p.rollbacks, f)
}

func (p *rollbacksPull) rollback() {
	if p.success {
		return
	}
	for _, r := range p.rollbacks {
		if err := r(); err != nil {
			log.Println(fmt.Sprintf(`failed to execute rollback: [err: %s]`, err))
		}
	}
}
