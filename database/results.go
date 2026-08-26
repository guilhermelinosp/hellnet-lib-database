package database

import "time"

// Result wraps a success/failure outcome with diagnostics. It mirrors the
// .NET DatabaseResult<T>: use Success or Failure to construct and IsSuccess
// to discriminate.
type Result[T any] struct {
	Data     T
	Err      error
	Duration time.Duration
}

// IsSuccess reports whether the operation completed without error.
func (r Result[T]) IsSuccess() bool { return r.Err == nil }

// Success builds a successful Result.
func Success[T any](data T, duration time.Duration) Result[T] {
	return Result[T]{Data: data, Duration: duration}
}

// Failure builds a failed Result.
func Failure[T any](err error, duration time.Duration) Result[T] {
	return Result[T]{Err: err, Duration: duration}
}

// PageResult is a paginated result. It mirrors the .NET PageResult<T>.
type PageResult[T any] struct {
	Items      []T
	TotalCount int64
	Page       int
	PageSize   int
}

// HasNextPage reports whether there are more pages after this one.
func (p PageResult[T]) HasNextPage() bool {
	return int64(p.Page*p.PageSize) < p.TotalCount
}
