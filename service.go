package vstate

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/google/uuid"
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
	VehicleStateReady VehicleState = "Ready"
	// The vehicle is low on battery but otherwise operational.
	// The vehicle cannot be claimed by an end­user but can be claimed by a hunter.
	VehicleStateBatteryLow VehicleState = "Battery_low"
	// Only available for “Hunters” to be picked up for charging.
	VehicleStateBounty VehicleState = "Bounty"
	// An end user is currently using this vehicle; it can not be claimed by another user or hunter.
	VehicleStateRiding VehicleState = "Riding"
	// A hunter has picked up a vehicle for charging.
	VehicleStateCollected VehicleState = "Collected"
	// A hunter has returned a vehicle after being charged.
	VehicleStateDropped VehicleState = "Dropped"
	/*
	   Not commissioned for service , not claimable by either end­users nor hunters.
	*/
	VehicleStateServiceMode VehicleState = "Service_mode"
	VehicleStateTerminated  VehicleState = "Terminated"
	VehicleStateUnknown     VehicleState = "Unknown"
)

type Vehicle struct {
	ID                 string
	State              VehicleState
	LastStateUpdatedAt time.Time
	Handled            bool
	TimeLocation       time.Location
	BatteryLevel       float64
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
	UpdateStateAndLastStateUpdatedAt(ctx context.Context, id string, s VehicleState, t time.Time) error
	UpdateState(ctx context.Context, id string, s VehicleState) error
	UpdateHandled(ctx context.Context, id string, handled bool) error
	GetVehicle(ctx context.Context, id string) (Vehicle, error)
}

type vehicleHandlingLockRepo interface {
	Create(ctx context.Context, doc vehicleHandlingLock) error
	Delete(ctx context.Context, id string) error
}

type VehicleStateChangeHandler struct {
	handledVehiclesLimit int
	batteryLimitForReady float64
	vehicleRepo          vehicleRepo
	lockRepo             vehicleHandlingLockRepo
	subscriber           subscriber
	publisher            publisher
	tickGetter           tickGetter
	wg                   *sync.WaitGroup
	waitResponseTimeout  time.Duration
	closeChan            chan struct{}
}

type tickGetter interface {
	GetUnknownStateTick(lastUpdatedAt time.Time) chan time.Time
	GetLateTick(location time.Location) chan time.Time
}

type subscriber interface {
	Subscribe(ctx context.Context, topic string) (<-chan VehicleStateChangeRequest, error)
}

type publisher interface {
	Publish(ctx context.Context, topic string, request VehicleStateChangeRequest) error
}

type VehicleStateChangeRequest struct {
	LockID       string
	State        VehicleState
	Operation    RequestOperation
	LockTTL      time.Duration
	receivedAt   time.Time
	ResponseChan chan<- VehicleStateChangeResponse
}

const (
	DefaultLockTTL = 10 * time.Second
)

type RequestOperation string

const (
	RequestOperationGet        = "get"
	RequestOperationGetAndLock = "get_and_lock"
	RequestOperationUpdate     = "update"
)

type VehicleStateChangeResponse struct {
	LockID string
	Error  string
	State  VehicleState
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

func (h *VehicleStateChangeHandler) GetState(ctx context.Context, vehicleID string) (VehicleState, error) {
	responseChan := make(chan VehicleStateChangeResponse, 1)
	req := VehicleStateChangeRequest{Operation: RequestOperationGet, ResponseChan: responseChan}
	if err := h.publisher.Publish(ctx, vehicleID, req); err != nil {
		return "", errors.Wrapf(err, `failed to publish request: [req: %+v, topic: %s]`, req, vehicleID)
	}
	resp, err := waitResponseWithTimeout(responseChan, h.waitResponseTimeout)
	if err != nil {
		return "", err
	}
	if resp.Error != "" {
		return "", errors.New(resp.Error)
	}
	return resp.State, nil
}

type UserRole string

const (
	UserRoleAdmin   UserRole = "Admin"
	UserRoleEndUser UserRole = "End-user"
	UserRoleHunter  UserRole = "Hunter"
)

func (h *VehicleStateChangeHandler) UpdateState(ctx context.Context, vehicleID string, s VehicleState, r UserRole) error {
	respChan := make(chan VehicleStateChangeResponse, 1)
	req := VehicleStateChangeRequest{Operation: RequestOperationGetAndLock, ResponseChan: respChan}
	if err := h.publisher.Publish(ctx, vehicleID, req); err != nil {
		return errors.Wrapf(err, `failed to publish request: [req: %+v]`, err)
	}
	resp, err := waitResponseWithTimeout(respChan, h.waitResponseTimeout)
	if err != nil {
		return err
	}
	if resp.State == s {
		return nil
	}
	if err = checkIsStateCanBeChanged(resp.State, s, r); err != nil {
		return err
	}
	respChan = make(chan VehicleStateChangeResponse, 1)
	req = VehicleStateChangeRequest{Operation: RequestOperationUpdate, State: s, ResponseChan: respChan, LockID: resp.LockID}
	if err = h.publisher.Publish(ctx, vehicleID, req); err != nil {
		return errors.Wrapf(err, `failed to publish request: [req: %+v]`, err)
	}
	resp, err = waitResponseWithTimeout(respChan, h.waitResponseTimeout)
	if err != nil {
		return err
	}
	if resp.Error != "" {
		return errors.New(resp.Error)
	}
	return nil
}

type ErrNotAllowedStateChange struct {
	message string
}

func (e ErrNotAllowedStateChange) Error() string {
	return e.message
}

func NewErrNotAllowedStateChange(r UserRole, current, required VehicleState) ErrNotAllowedStateChange {
	return ErrNotAllowedStateChange{message: fmt.Sprintf(`user with role %s can not change state from %s to %s`, r, current, required)}
}

func checkIsStateCanBeChanged(current, required VehicleState, r UserRole) error {
	if r == UserRoleAdmin {
		return nil
	}

	if r == UserRoleEndUser {
		allowedStatesForEndUsers := map[VehicleState]bool{
			VehicleStateRiding: true,
			VehicleStateReady:  true,
		}
		if !allowedStatesForEndUsers[current] || !allowedStatesForEndUsers[required] {
			return NewErrNotAllowedStateChange(r, current, required)
		}
	}
	return nil
}

type ErrWaitingResponseTimeout struct {
}

func (ErrWaitingResponseTimeout) Error() string {
	return "Timeout for waiting response exceed"
}

func waitResponseWithTimeout(c <-chan VehicleStateChangeResponse, t time.Duration) (VehicleStateChangeResponse, error) {
	select {
	case <-time.After(t):
		return VehicleStateChangeResponse{}, ErrWaitingResponseTimeout{}
	case resp := <-c:
		return resp, nil
	}
}

func (h *VehicleStateChangeHandler) handleStateChange(ctx context.Context, v Vehicle, requestChan <-chan VehicleStateChangeRequest) {
	unknownStateTick := h.tickGetter.GetUnknownStateTick(v.LastStateUpdatedAt)
	lateTick := h.tickGetter.GetLateTick(v.TimeLocation)
	var lateTickDone bool
	currentState := v.State
	var lockID string
	var waitingLocks []VehicleStateChangeRequest
	lockTTLChan := make(<-chan time.Time, 0)
	for {
		select {
		case <-ctx.Done():
			if err := h.vehicleRepo.UpdateHandled(context.Background(), v.ID, false); err != nil {
				log.Println(fmt.Sprintf(`failed to update handled to false: [vehicle_id: %s]`, v.ID))
			}
		case <-unknownStateTick:
			currentState = VehicleStateUnknown
			if err := h.vehicleRepo.UpdateState(ctx, v.ID, currentState); err != nil {
				log.Println(fmt.Sprintf(`failed to update vehicle state: [vehicle_id: %s, state: %s]`, v.ID, currentState))
			}
		case <-lateTick:
			if currentState != VehicleStateReady {
				lateTickDone = true
				break
			}
			currentState = VehicleStateBounty
			if err := h.vehicleRepo.UpdateState(ctx, v.ID, currentState); err != nil {
				log.Println(fmt.Sprintf(`failed to update vehicle state: [vehicle_id: %s, state: %s]`, v.ID, currentState))
			}
		case <-lockTTLChan:
			lockID = ""
			indexToTruncate := len(waitingLocks)
			for i, req := range waitingLocks {
				sub := time.Now().Sub(req.receivedAt) - req.LockTTL
				if sub < 0 {
					continue
				}
				indexToTruncate = i + 1
				lockID = uuid.New().String()
				lockTTLChan = time.After(sub)
				req.ResponseChan <- VehicleStateChangeResponse{LockID: lockID, State: currentState}
				close(req.ResponseChan)
				break
			}
			waitingLocks = waitingLocks[indexToTruncate:]
		case req := <-requestChan:
			switch req.Operation {
			case RequestOperationGet:
				req.ResponseChan <- VehicleStateChangeResponse{State: currentState}
			case RequestOperationGetAndLock:
				if req.LockTTL == 0 {
					req.LockTTL = DefaultLockTTL
				}
				if lockID != "" {
					req.receivedAt = time.Now()
					waitingLocks = append(waitingLocks, req)
					break
				}
				lockID = uuid.New().String()
				lockTTLChan = time.After(req.LockTTL)
				req.ResponseChan <- VehicleStateChangeResponse{LockID: lockID, State: currentState}
			case RequestOperationUpdate:
				if req.LockID != lockID {
					req.ResponseChan <- VehicleStateChangeResponse{Error: fmt.Sprintf(`wrong lock id: %s`, req.LockID)}
					break
				}
				bounty, err := h.shouldSetBounty(ctx, v.ID, req.State, currentState, lateTickDone)
				if err != nil {
					req.ResponseChan <- VehicleStateChangeResponse{Error: fmt.Sprintf(`failed to check should set bounty: [error: %s]`, err)}
				}
				currentState = req.State
				if bounty {
					currentState = VehicleStateBounty
				}
				now := time.Now().UTC()
				if err = h.vehicleRepo.UpdateStateAndLastStateUpdatedAt(ctx, v.ID, currentState, now); err != nil {
					req.ResponseChan <- VehicleStateChangeResponse{Error: fmt.Sprintf(`failed to update vehicle state: [vehicle_id: %s, state: %s]`, v.ID, currentState)}
				}
				unknownStateTick = h.tickGetter.GetUnknownStateTick(now)
			default:
				req.ResponseChan <- VehicleStateChangeResponse{Error: fmt.Sprintf(`unsupported operation: %s`, req.Operation)}
			}
			close(req.ResponseChan)
		}
	}
	h.wg.Done()
	return
}

func (h *VehicleStateChangeHandler) shouldSetBounty(ctx context.Context, vehicleID string, requiredState, currentState VehicleState, lateTickDone bool) (bool, error) {
	if requiredState == VehicleStateReady && lateTickDone {
		return true, nil
	}
	if currentState != VehicleStateRiding {
		return false, nil
	}
	v, err := h.vehicleRepo.GetVehicle(ctx, vehicleID)
	if err != nil {
		return false, errors.Wrapf(err, `failed to get vehicle: [id: %s]`, vehicleID)
	}
	if v.BatteryLevel < h.batteryLimitForReady {
		return true, nil
	}
	return false, nil
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
