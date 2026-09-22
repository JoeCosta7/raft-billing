package api

import (
	"fmt"
	"net/http"
	"strconv"
)

const (
	defaultLimit = 50
	maxLimit     = 200
)

func parsePagination(r *http.Request) (limit int, cursor string, err error) {
	limit = defaultLimit
	if s := r.URL.Query().Get("limit"); s != "" {
		n, convErr := strconv.Atoi(s)
		if convErr != nil || n < 1 || n > maxLimit {
			return 0, "", fmt.Errorf("limit must be an integer between 1 and %d", maxLimit)
		}
		limit = n
	}
	cursor = r.URL.Query().Get("cursor")
	return limit, cursor, nil
}

// pageResponse is the response envelope for every paginated list endpoint.
type pageResponse[T any] struct {
	Items      []T    `json:"items"`
	NextCursor string `json:"next_cursor,omitempty"`
}
