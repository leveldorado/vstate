package vstate

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/leveldorado/vstate/user"
	"github.com/pkg/errors"
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

type Vehicle struct {
	ID                 string
	State              State
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

type batteryLevelChecker interface {
	CheckBatteryLevel(ctx context.Context, vehicleID string) (float64, error)
}

type vehicleRepo interface {
	ListNotHandled(ctx context.Context, limit int) ([]Vehicle, error)
	UpdateStateAndLastStateUpdatedAt(ctx context.Context, id string, s State, t time.Time) error
	UpdateState(ctx context.Context, id string, s State) error
	UpdateHandled(ctx context.Context, id string, handled bool) error
	GetVehicle(ctx context.Context, id string) (Vehicle, error)
}

type vehicleHandlingLockRepo interface {
	Create(ctx context.Context, doc vehicleHandlingLock) error
	Delete(ctx context.Context, id string) error
}

type tickGetter interface {
	GetUnknownStateTick(lastUpdatedAt time.Time) chan time.Time
	GetLateTick(location time.Location) chan time.Time
}

type subscriber interface {
	Subscribe(ctx context.Context, topic string) (<-chan ChangeRequest, error)
}

type publisher interface {
	Publish(ctx context.Context, topic string, request ChangeRequest) error
}

type ChangeRequest struct {
	LockID       string
	State        State
	Operation    RequestOperation
	LockTTL      time.Duration
	receivedAt   time.Time
	ResponseChan chan<- ChangeResponse
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

type ChangeResponse struct {
	LockID string
	Error  string
	State  State
}

type ChangeHandler struct {
	config              ChangeHandlerConfig
	vehicleRepo         vehicleRepo
	lockRepo            vehicleHandlingLockRepo
	batteryLevelChecker batteryLevelChecker
	subscriber          subscriber
	publisher           publisher
	tickGetter          tickGetter
	wg                  *sync.WaitGroup
}

type ChangeHandlerConfig struct {
	HandleVehiclesLimit int
	BatterLimitForReady float64
	WaitResponseTimeout time.Duration
}

func NewChangeHandler(ctx context.Context, vr vehicleRepo, lr vehicleHandlingLockRepo, b batteryLevelChecker, s subscriber, p publisher, t tickGetter, cfg ChangeHandlerConfig, wg *sync.WaitGroup) (*ChangeHandler, error) {
	c := &ChangeHandler{
		config:              cfg,
		vehicleRepo:         vr,
		lockRepo:            lr,
		batteryLevelChecker: b,
		subscriber:          s,
		publisher:           p,
		tickGetter:          t,
		wg:                  wg,
	}
	return c, c.init(ctx)
}

func (h *ChangeHandler) init(ctx context.Context) error {
	rp := &rollbacksPull{}
	defer rp.rollback()
	vehiclesToHandle, err := h.getVehiclesToHandle(ctx, rp)
	if err != nil {
		return errors.Wrap(err, `failed to get vehicles to handle`)
	}
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

func (h *ChangeHandler) GetState(ctx context.Context, vehicleID string) (State, error) {
	responseChan := make(chan ChangeResponse, 1)
	req := ChangeRequest{Operation: RequestOperationGet, ResponseChan: responseChan}
	if err := h.publisher.Publish(ctx, vehicleID, req); err != nil {
		return "", errors.Wrapf(err, `failed to publish request: [req: %+v, topic: %s]`, req, vehicleID)
	}
	resp, err := waitResponseWithTimeout(responseChan, h.config.WaitResponseTimeout)
	if err != nil {
		return "", err
	}
	if resp.Error != "" {
		return "", errors.New(resp.Error)
	}
	return resp.State, nil
}

func (h *ChangeHandler) UpdateState(ctx context.Context, vehicleID string, s State, r user.Role) error {
	respChan := make(chan ChangeResponse, 1)
	req := ChangeRequest{Operation: RequestOperationGetAndLock, ResponseChan: respChan}
	if err := h.publisher.Publish(ctx, vehicleID, req); err != nil {
		return errors.Wrapf(err, `failed to publish request: [req: %+v]`, err)
	}
	resp, err := waitResponseWithTimeout(respChan, h.config.WaitResponseTimeout)
	if err != nil {
		return err
	}
	if resp.State == s {
		return nil
	}
	if err = checkIsStateCanBeChanged(resp.State, s, r); err != nil {
		return err
	}
	if s == Ready {
		batteryLevel, err := h.batteryLevelChecker.CheckBatteryLevel(ctx, vehicleID)
		if err != nil {
			return errors.Wrapf(err, `failed to check battery level: [vehicle_id: %s]`, vehicleID)
		}
		if batteryLevel < h.config.BatterLimitForReady {
			s = BatteryLow
		}
	}
	respChan = make(chan ChangeResponse, 1)
	req = ChangeRequest{Operation: RequestOperationUpdate, State: s, ResponseChan: respChan, LockID: resp.LockID}
	if err = h.publisher.Publish(ctx, vehicleID, req); err != nil {
		return errors.Wrapf(err, `failed to publish request: [req: %+v]`, err)
	}
	resp, err = waitResponseWithTimeout(respChan, h.config.WaitResponseTimeout)
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

func NewErrNotAllowedStateChange(r user.Role, current, required State) ErrNotAllowedStateChange {
	return ErrNotAllowedStateChange{message: fmt.Sprintf(`user with role %s can not change state from %s to %s`, r, current, required)}
}

func checkIsStateCanBeChanged(current, required State, r user.Role) error {
	if r == user.RoleAdmin {
		return nil
	}

	if r == user.RoleEndUser {
		allowedStatesForEndUsers := map[State]bool{
			Riding: true,
			Ready:  true,
		}
		if !allowedStatesForEndUsers[current] || !allowedStatesForEndUsers[required] {
			return NewErrNotAllowedStateChange(r, current, required)
		}
	}
	switch current {
	case Ready:
		if required == Riding {
			return nil
		}
	case Riding, Dropped:
		if required == Ready {
			return nil
		}
	case Collected:
		if required == Dropped {
			return nil
		}
	case Bounty:
		if required == Collected {
			return nil
		}
	}
	return NewErrNotAllowedStateChange(r, current, required)
}

type ErrWaitingResponseTimeout struct {
}

func (ErrWaitingResponseTimeout) Error() string {
	return "Timeout for waiting response exceed"
}

func waitResponseWithTimeout(c <-chan ChangeResponse, t time.Duration) (ChangeResponse, error) {
	select {
	case <-time.After(t):
		return ChangeResponse{}, ErrWaitingResponseTimeout{}
	case resp := <-c:
		return resp, nil
	}
}

func (h *ChangeHandler) handleStateChange(ctx context.Context, v Vehicle, requestChan <-chan ChangeRequest) {
	unknownStateTick := h.tickGetter.GetUnknownStateTick(v.LastStateUpdatedAt)
	lateTick := h.tickGetter.GetLateTick(v.TimeLocation)
	var lateTickDone bool
	currentState := v.State
	var lockID string
	var waitingLocks []ChangeRequest
	lockTTLChan := make(<-chan time.Time, 0)
	for {
		select {
		case <-ctx.Done():
			if err := h.vehicleRepo.UpdateHandled(context.Background(), v.ID, false); err != nil {
				log.Println(fmt.Sprintf(`failed to update handled to false: [vehicle_id: %s]`, v.ID))
			}
		case <-unknownStateTick:
			currentState = Unknown
			if err := h.vehicleRepo.UpdateState(ctx, v.ID, currentState); err != nil {
				log.Println(fmt.Sprintf(`failed to update vehicle state: [vehicle_id: %s, state: %s]`, v.ID, currentState))
			}
		case <-lateTick:
			if currentState != Ready {
				lateTickDone = true
				break
			}
			currentState = Bounty
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
				req.ResponseChan <- ChangeResponse{LockID: lockID, State: currentState}
				close(req.ResponseChan)
				break
			}
			waitingLocks = waitingLocks[indexToTruncate:]
		case req := <-requestChan:
			switch req.Operation {
			case RequestOperationGet:
				req.ResponseChan <- ChangeResponse{State: currentState}
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
				req.ResponseChan <- ChangeResponse{LockID: lockID, State: currentState}
			case RequestOperationUpdate:
				if req.LockID != lockID {
					req.ResponseChan <- ChangeResponse{Error: fmt.Sprintf(`wrong lock id: %s`, req.LockID)}
					break
				}
				bounty, err := h.shouldSetBounty(ctx, v.ID, req.State, currentState, lateTickDone)
				if err != nil {
					req.ResponseChan <- ChangeResponse{Error: fmt.Sprintf(`failed to check should set bounty: [error: %s]`, err)}
				}
				currentState = req.State
				if bounty {
					currentState = Bounty
				}
				now := time.Now().UTC()
				if err = h.vehicleRepo.UpdateStateAndLastStateUpdatedAt(ctx, v.ID, currentState, now); err != nil {
					req.ResponseChan <- ChangeResponse{Error: fmt.Sprintf(`failed to update vehicle state: [vehicle_id: %s, state: %s]`, v.ID, currentState)}
				}
				unknownStateTick = h.tickGetter.GetUnknownStateTick(now)
			default:
				req.ResponseChan <- ChangeResponse{Error: fmt.Sprintf(`unsupported operation: %s`, req.Operation)}
			}
			close(req.ResponseChan)
		}
	}
	h.wg.Done()
	return
}

func (h *ChangeHandler) shouldSetBounty(ctx context.Context, vehicleID string, requiredState, currentState State, lateTickDone bool) (bool, error) {
	if requiredState == Ready && lateTickDone {
		return true, nil
	}
	if currentState != BatteryLow {
		return false, nil
	}
	return true, nil
}

func (h *ChangeHandler) getVehiclesToHandle(ctx context.Context, rp *rollbacksPull) ([]Vehicle, error) {
	var vehiclesToHandle []Vehicle
	for {
		if len(vehiclesToHandle) == h.config.HandleVehiclesLimit {
			break
		}
		limit := h.config.HandleVehiclesLimit - len(vehiclesToHandle)
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

func (h *ChangeHandler) acquireVehicleForHandling(ctx context.Context, id string, rp *rollbacksPull) (bool, error) {
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
