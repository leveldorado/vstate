package vstate

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/pkg/errors"
)

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
	ID                 string
	State              VehicleState
	LastStateUpdatedAt time.Time
	Handled            bool
	TimeLocation       time.Location
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
	UpdateHandled(ctx context.Context, id string, handled bool) error
}

type vehicleHandlingLockRepo interface {
	Create(ctx context.Context, doc vehicleHandlingLock) error
	Delete(ctx context.Context, id string) error
}

type VehicleStateChangeHandler struct {
	handledVehiclesLimit   int
	vehicleRepo            vehicleRepo
	lockRepo               vehicleHandlingLockRepo
	subscriber             subscriber
	publisher              publisher
	unknownStateTickGetter unknownStateTickGetter
	wg                     *sync.WaitGroup
}

type unknownStateTickGetter interface {
	GetUnknownStateTick(lastUpdatedAt time.Time) chan time.Time
}

type subscriber interface {
	Subscribe(ctx context.Context, topic string) (<-chan VehicleStateChangeRequest, error)
}

type publisher interface {
	Publish(ctx context.Context, topic string, request VehicleStateChangeRequest) error
}

type VehicleStateChangeRequest struct {
	VehicleID    string
	State        VehicleState
	ResponseChan chan<- VehicleStateChangeResponse
}

type VehicleStateChangeResponse struct {
	LockID string
	Error  string
}

func (h *VehicleStateChangeHandler) Init(ctx context.Context) error {
	rp := &rollbacksPull{}
	defer rp.rollback()
	vehiclesToHandle, err := h.getVehiclesToHandle(ctx, rp)
	if err != nil {
		return errors.Wrap(err, `failed to get vehicles to handle`)
	}
	h.wg = &sync.WaitGroup{}
	for _, v := range vehiclesToHandle {
		ch, err := h.subscriber.Subscribe(ctx, v.ID)
		if err != nil {
			return errors.Wrapf(err, `failed to subscribe to vehicle state change: [id: %s]`, v.ID)
		}
		go h.handleStateChange(ctx, v, ch)
	}
	h.wg.Add(len(vehiclesToHandle))
	rp.commit()
	return nil
}

func (h *VehicleStateChangeHandler) handleStateChange(ctx context.Context, v Vehicle, requestChan <-chan VehicleStateChangeRequest) {

	return
}

func (h *VehicleStateChangeHandler) getVehiclesToHandle(ctx context.Context, rp *rollbacksPull) ([]Vehicle, error) {
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

func (h *VehicleStateChangeHandler) acquireVehicleForHandling(ctx context.Context, id string, rp *rollbacksPull) (bool, error) {
	lockEntry := vehicleHandlingLock{VehicleID: id, CreatedAt: time.Now().UTC()}
	err := h.lockRepo.Create(ctx, lockEntry)
	switch errors.Cause(err).(type) {
	case ErrPrimaryKeyDuplicated:
		return false, nil
	case nil:
	default:
		return false, errors.Wrapf(err, `failed to save vehicle handling lock: [doc: %+v]`, lockEntry)
	}
	rp.addRollback(func() error {
		return errors.Wrapf(h.lockRepo.Delete(ctx, id), `failed to delete vehicle handling lock: [id: %s]`, id)
	})
	if err = h.vehicleRepo.UpdateHandled(ctx, id, true); err != nil {
		return false, errors.Wrapf(err, `failed to update handled: [id: %s, handled: %v]`, id, true)
	}
	rp.addRollback(func() error {
		return errors.Wrapf(h.vehicleRepo.UpdateHandled(ctx, id, false), `failed to update vehicle: [id: %s, handled: %v]`, id, false)
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
