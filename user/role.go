package user

type Role string

const (
	RoleAdmin   Role = "Admin"
	RoleEndUser Role = "End-user"
	RoleHunter  Role = "Hunter"
)
