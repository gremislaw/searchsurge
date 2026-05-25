package shared

import "errors"

var (
	ErrNotFound      = errors.New("resource not found")
	ErrAlreadyExists = errors.New("resource already exists")
	ErrInvalidInput  = errors.New("invalid input")
	ErrRateLimited   = errors.New("rate limit exceeded")
)

func IsBusinessError(err error) bool {
	return errors.Is(err, ErrNotFound) ||
		errors.Is(err, ErrAlreadyExists) ||
		errors.Is(err, ErrInvalidInput) ||
		errors.Is(err, ErrRateLimited)
}