package user



type Role string

const (
	RoleEndUser Role = "End-user"
	RoleHunter Role = "Hunter"
	RoleAdmin Role = "Admin"
)
